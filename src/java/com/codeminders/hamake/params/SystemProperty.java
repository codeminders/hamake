package com.codeminders.hamake.params;

import java.io.IOException;

import com.codeminders.hamake.context.Context;

public class SystemProperty implements Parameter{
	
	private String name;
	private String value;
	
	public SystemProperty(String name, String value){
		this.name = name;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String get(Context context) throws IOException {
		return "-D" + name + "=" + value;
	}

}
