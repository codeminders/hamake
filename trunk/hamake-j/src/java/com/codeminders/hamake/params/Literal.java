package com.codeminders.hamake.params;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.dtr.Foreach;

public class Literal{
	private String value;
	
	public Literal(String value){
		this.value = value;
	}
	
	public String getValue(Context context){
		return Utils.replaceVariables(context, value); 
	}
	
}