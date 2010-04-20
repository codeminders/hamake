package com.codeminders.hamake;

import java.util.HashMap;
import java.util.Map;

public class Context {
	
	Map<String, Object> nameValuePairs;
	
	public Context(){
		this(null);
	}
	
	public Context(Context parentContext){
		nameValuePairs = new HashMap<String, Object>();
		if(parentContext != null){
			for(Map.Entry<String, Object> entry : parentContext.getNameValuePairs().entrySet())
			nameValuePairs.put(entry.getKey(), entry.getValue());
		}
	}
	
	protected Map<String, Object> getNameValuePairs(){
		return nameValuePairs;
	}
	
	public void set(String name, Object value){
		nameValuePairs.put(name, value);
	}
	
	public Object get(String name){
		return nameValuePairs.get(name);
	}
	
	public String getString(String name){
		if(nameValuePairs.get(name) instanceof String){
			return (String)nameValuePairs.get(name);
		}
		return null;
	}

}
