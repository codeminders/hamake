package com.codeminders.hamake.params;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.data.DataFunction;

public class Reference{
	private String refid;
	
	public Reference(String refid){
		this.refid = refid;
	}
	
	public String getValue(Context context) throws IOException{
		Object obj = context.get(refid);
		if(obj instanceof DataFunction){
			List<Path> paths = ((DataFunction)obj).getPath(context);
			return StringUtils.join(paths, ",");
		}
		return null;
	}
}
