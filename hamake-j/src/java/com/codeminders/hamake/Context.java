package com.codeminders.hamake;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class Context {

	public static final String SYSTEM_VARS_PREFIX = "sys:";
	public static final String ENVIRONMENT_VARS_PREFIX = "env:";
	public static final String HAMAKE_VARS_PREFIX = "hamake:";
	public static final String FOREACH_VARS_PREFIX = "foreach:";
	public static final String FOLD_VARS_PREFIX = "fold:";
	
	public static final String[] FORBIDDEN_PREFIXES = {SYSTEM_VARS_PREFIX, ENVIRONMENT_VARS_PREFIX, HAMAKE_VARS_PREFIX, FOREACH_VARS_PREFIX, FOLD_VARS_PREFIX};

	Map<String, Object> nameValuePairs;

	public Context() {
		this(null);
	}

	public Context(Context parentContext) {
		nameValuePairs = new HashMap<String, Object>();
		if (parentContext != null) {
			for (Map.Entry<String, Object> entry : parentContext
					.getNameValuePairs().entrySet())
				nameValuePairs.put(entry.getKey(), entry.getValue());
		} else {
			for (Map.Entry<String, String> envEntry : System.getenv()
					.entrySet()) {
				nameValuePairs.put(ENVIRONMENT_VARS_PREFIX + envEntry.getKey(),
						envEntry.getValue());
			}
		}
	}

	protected Map<String, Object> getNameValuePairs() {
		return nameValuePairs;
	}

	public void set(String name, Object value)
			throws InvalidContextVariableException {
		for(String forbiddenPrefix : FORBIDDEN_PREFIXES){
			if(StringUtils.startsWithIgnoreCase(name, forbiddenPrefix)){
				throw new InvalidContextVariableException("variable " + name + " starts with forbidden prefix " + forbiddenPrefix);
			}
		}
		if (!nameValuePairs.containsKey(name)) {
			nameValuePairs.put(name, value);
		}
		else{
			throw new InvalidContextVariableException("error setting " + name + ". Could not modify context");
		}
	}

	public void setSystem(String name, Object value) {
		if (!nameValuePairs.containsKey(SYSTEM_VARS_PREFIX + name)) {
			nameValuePairs.put(SYSTEM_VARS_PREFIX + name, value);
		}
	}
	
	public Object getSystem(String key){
		return nameValuePairs.get(SYSTEM_VARS_PREFIX + key);
	}

	public void setHamake(String name, Object value) {
		if (!nameValuePairs.containsKey(HAMAKE_VARS_PREFIX + name)) {
			nameValuePairs.put(HAMAKE_VARS_PREFIX + name, value);
		}
	}
	
	public Object getHamake(String key){
		return nameValuePairs.get(HAMAKE_VARS_PREFIX + key);
	}
	
	public void setForeach(String name, Object value) {
		if (!nameValuePairs.containsKey(FOREACH_VARS_PREFIX + name)) {
			nameValuePairs.put(FOREACH_VARS_PREFIX + name, value);
		}
	}
	
	public Object getForeach(String key){
		return nameValuePairs.get(FOREACH_VARS_PREFIX + key);
	}

	public Object get(String name) {
		return nameValuePairs.get(name);
	}
	
	public String getString(String name) {
		Object obj = get(name);
		if (obj != null && obj instanceof String) {
			return (String)obj;
		}
		return null;
	}

}