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
		return Utils.replaceVariables(value, context, 
				Foreach.FULL_FILENAME,
				Foreach.SHORT_FILENAME,
				Foreach.PARENT_FOLDER,
				Foreach.FILENAME_WO_EXTENTION,
				Foreach.EXTENTION);
	}
	
}
