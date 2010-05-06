package com.codeminders.hamake;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class Context {

	public static final String SYSTEM_VARS_PREFIX = "sys:";
	public static final String ENVIRONMENT_VARS_PREFIX = "env:";
	public static final String HAMAKE_VARS_PREFIX = "hamake:";
	public static final String FOREACH_VARS_PREFIX = "foreach:";
	public static final String HADOOP_VARS_PREFIX = "hadoop:";
	public static final String FOLD_VARS_PREFIX = "fold:";
	
	public static final String HAMAKE_PROPERTY_WORKING_FOLDER = HAMAKE_VARS_PREFIX + "workdir";
	public static final String HAMAKE_PROPERTY_HADOOP_CONFIGURATION = HAMAKE_VARS_PREFIX + "hadoop.configuration";
	public static final String HAMAKE_PROPERTY_HAMAKE_VERSION = HAMAKE_VARS_PREFIX + "version";
	public static final String HAMAKE_PROPERTY_WITH_DEPENDENCIES = HAMAKE_VARS_PREFIX + "dependencies.enabled";
	
	public static final String HADOOP_PROPERTY_TEMP_FOLDER = HADOOP_VARS_PREFIX + "temp.folder";
	
	public static final String[] FORBIDDEN_PREFIXES = {SYSTEM_VARS_PREFIX, ENVIRONMENT_VARS_PREFIX, HADOOP_VARS_PREFIX, HAMAKE_VARS_PREFIX, FOREACH_VARS_PREFIX, FOLD_VARS_PREFIX};

	private Map<String, Object> nameValuePairs;

	private Context() throws InvalidContextStateException {
		super();
	}
	
	public static Context initContext(Configuration hadoopConf, String workDir, String hamakeVersion, boolean dependenciesEnabled) throws InvalidContextStateException{
		Context context = new Context();
		context.nameValuePairs = new HashMap<String, Object>();
		context.setForbidden(HAMAKE_PROPERTY_HADOOP_CONFIGURATION, new Configuration());
		context.setForbidden(HAMAKE_PROPERTY_WORKING_FOLDER, workDir);
		context.setForbidden(HAMAKE_PROPERTY_HAMAKE_VERSION, hamakeVersion);
		context.setForbidden(HAMAKE_PROPERTY_WITH_DEPENDENCIES, dependenciesEnabled);
		return context;
	}

	public Context(Context parentContext) throws InvalidContextStateException {
		if(parentContext == null) throw new InvalidContextStateException("Parent context can not be null");
		nameValuePairs = new HashMap<String, Object>();
		for (Map.Entry<String, Object> entry : parentContext.getNameValuePairs().entrySet())
			nameValuePairs.put(entry.getKey(), entry.getValue());
	}

	protected Map<String, Object> getNameValuePairs() {
		return nameValuePairs;
	}

	public void set(String name, Object value)
			throws InvalidContextStateException {
		for(String forbiddenPrefix : FORBIDDEN_PREFIXES){
			if(StringUtils.startsWithIgnoreCase(name, forbiddenPrefix)){
				throw new InvalidContextStateException("variable " + name + " starts with forbidden prefix " + forbiddenPrefix);
			}
		}
		if (!nameValuePairs.containsKey(name)) {
			nameValuePairs.put(name, value);
		}
		else{
			throw new InvalidContextStateException("error setting " + name + ". Could not modify context");
		}
	}

	public void setForbidden(String name, Object value) {
		if (!nameValuePairs.containsKey(name)) {
			nameValuePairs.put(name, value);
		}
	}
	
	public Object get(String name) {
		if(StringUtils.startsWithIgnoreCase(name, ENVIRONMENT_VARS_PREFIX)) return System.getenv(StringUtils.split(name, ":")[1]);
		else if(StringUtils.startsWithIgnoreCase(name, HADOOP_VARS_PREFIX)){
			Configuration conf = (Configuration)get(HAMAKE_PROPERTY_HADOOP_CONFIGURATION);
			return conf.get(StringUtils.split(name, ":")[1]);
		}
		else return nameValuePairs.get(name);
	}
	
	public String getString(String name) {
		Object obj = get(name);
		if (obj != null && obj instanceof String) {
			return (String)obj;
		}
		return null;
	}

}
