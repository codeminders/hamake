package com.codeminders.hamake.context;

import java.util.HashMap;
import java.util.Map;

import net.sf.ehcache.CacheManager;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.InvalidContextStateException;

public class Context {
	
	public static CacheManager cacheManager = CacheManager.create();
	
	public Context parent;

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
	public static final String HAMAKE_PROPERTY_VERBOSE = HAMAKE_VARS_PREFIX + "verbose";
	public static final String HAMAKE_PROPERTY_DRY_RUN = HAMAKE_VARS_PREFIX + "dry.run";
	public static final String HAMAKE_PROPERTY_VALIDATOR_SCHEMA_TYPE = HAMAKE_VARS_PREFIX + "schema.type";
	
	public static final String HADOOP_PROPERTY_TEMP_FOLDER = HADOOP_VARS_PREFIX + "temp.folder";
	
	public static final String[] FORBIDDEN_PREFIXES = {SYSTEM_VARS_PREFIX, ENVIRONMENT_VARS_PREFIX, HADOOP_VARS_PREFIX, HAMAKE_VARS_PREFIX, FOREACH_VARS_PREFIX, FOLD_VARS_PREFIX};

	private Map<String, Object> nameValuePairs = new HashMap<String, Object>();

	public Context(Configuration hadoopConf, String workDir, boolean dependenciesEnabled, boolean verbose, boolean dryRun){
		setForbidden(HAMAKE_PROPERTY_HADOOP_CONFIGURATION, hadoopConf);
		setForbidden(HAMAKE_PROPERTY_WORKING_FOLDER, workDir);
		setForbidden(HAMAKE_PROPERTY_HAMAKE_VERSION, Hamake.HAMAKE_VERSION);
		setForbidden(HAMAKE_PROPERTY_WITH_DEPENDENCIES, dependenciesEnabled);
		setForbidden(HAMAKE_PROPERTY_VERBOSE, verbose);
		setForbidden(HAMAKE_PROPERTY_DRY_RUN, dryRun);
	}
	
	public Context(Context parentContext){
		this.parent = parentContext;
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
			throw new InvalidContextStateException("error setting variable " + name + ". Context is immutable");
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
		else if(nameValuePairs.containsKey(name))return nameValuePairs.get(name);
		else if(parent != null)return parent.get(name);
		else return null;
	}
	
	public String getString(String name) {
		Object obj = get(name);
		if (obj != null && obj instanceof String) {
			return (String)obj;
		}
		return null;
	}
	
	public Boolean getBoolean(String name) {
		Object obj = get(name);
		if (obj != null && obj instanceof Boolean) {
			return (Boolean)obj;
		}
		return null;
	}

}
