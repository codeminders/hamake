package com.codeminders.hamake.params;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import com.codeminders.hamake.HamakePath;

public class HamakeParameter implements Param{
	
	protected final Pattern inputPattern = Pattern.compile("\\$\\{input\\}");
	protected final Pattern outputPattern = Pattern.compile("\\$\\{output\\}");
	
	private String value;
	
	public HamakeParameter(String value){
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}

	public List<String> get(Map<String, List<HamakePath>> dict, FileSystem fs) throws IOException{
//		Matcher variableMatcher = SyntaxParser.VARIABLE_PATTERN.matcher(value);
		StringBuilder str = new StringBuilder(value);
		str = substr(str, inputPattern.matcher(str.toString()), dict.get(PathParam.Type.inputfile.toString()), dict.get(PathParam.Type.input.toString()));
		str = substr(str, outputPattern.matcher(str.toString()), dict.get(PathParam.Type.outputfile.toString()), dict.get(PathParam.Type.output.toString()));
		System.err.println("returning param: " + str.toString());
		return Arrays.asList(StringUtils.split(str.toString(), " "));
	}
	
	protected StringBuilder substr(StringBuilder value, Matcher matcher, List<HamakePath>... pathsLists){
		StringBuilder str = new StringBuilder();
		while(matcher.find()){
			int start = matcher.start();
			int end = matcher.end();			
			str.append(value, 0, start);
			for(List<HamakePath> pathsList : pathsLists){
				if(pathsList != null){
					for(HamakePath path : pathsList){
						str.append(path.getPathName().toString()).append(" ");
					}
				}
			}
			str.append(value, end, value.length());
		}	
		if(StringUtils.isEmpty(str.toString()))	return value;
		else return str;
	}
	
}
