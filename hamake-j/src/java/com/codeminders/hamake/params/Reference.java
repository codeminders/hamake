package com.codeminders.hamake.params;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Utils;

public class Reference{
	private String refid;
	
	public Reference(String refid){
		this.refid = refid;
	}
	
	public String getValue(Context context){
		String value = refid;
		return Utils.replaceVariables(value, context, refid);
	}
}
