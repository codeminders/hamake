package com.codeminders.hamake;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

public class HamakeParameter implements Param{
	
	private String value;
	
	public HamakeParameter(String value){
		this.value = value;
	}
	
	
	public String getValue() {
		return value;
	}

	public Collection<String> get(Map<String, Collection> dict, FileSystem fs) throws IOException{
		return Collections.singleton(value);
	}
}
