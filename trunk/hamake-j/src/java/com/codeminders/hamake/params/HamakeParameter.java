package com.codeminders.hamake.params;

import java.io.IOException;
import java.util.List;

import com.codeminders.hamake.Context;

public class HamakeParameter implements Parameter{
	
	public HamakeParameter(List<Object> values, ConcatFunction concatFunc, ProcessingFunction processingFunc){
		this.values = values;
		this.concatFunc = concatFunc;
		this.processingFunc = processingFunc;
	}
	
	private List<Object> values;
	private ConcatFunction concatFunc;
	private ProcessingFunction processingFunc;
	
	public String get(Context context) throws IOException{
		StringBuilder parameter = new StringBuilder();
		for(Object value : values){
			if(value instanceof Reference){
				Reference reference = (Reference)value;
				parameter.append(concatFunc.concat(parameter.toString(), processingFunc.process(reference.getValue(context))));
			}
			else if(value instanceof Literal){
				Literal literal = (Literal)value;
				parameter.append(concatFunc.concat(parameter.toString(), processingFunc.process(literal.getValue(context))));
			}
		}
		return parameter.toString();
	}
	
}
