package com.codeminders.hamake.params;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.commons.lang.StringUtils;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.DataFunction;

public class Reference extends ParameterItem{
	private String refid;
	
	public Reference(String refid){
		this.refid = refid;
	}
	
	public String getValue(Context context, ConcatFunction concatFunc) throws IOException{
		Object obj = context.get(refid);
		if(obj instanceof DataFunction){
			List<Path> paths = ((DataFunction)obj).getPath(context);
            String[] sPath = new String[paths.size()];
            int i = 0;
            for (Path p:paths)
            {
            	String s = p.toString();
                if (!StringUtils.isEmpty(s)){
                	sPath[i++] = p.toString();
                }
                    
            }

			//return StringUtils.join(paths, " ");
            return concatFunc.concat(sPath);
		}
		return null;
	}
}
