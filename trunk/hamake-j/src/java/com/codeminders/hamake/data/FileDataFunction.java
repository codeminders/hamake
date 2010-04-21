package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.Utils;

public class FileDataFunction extends DataFunction {
	
	private String path;
	
	public FileDataFunction(String id, int generation, long validityPeriod, String workFolder, String path) throws IOException{
		super(id, generation, validityPeriod, workFolder);
		this.path = path;
	}

	@Override
	public boolean clear(Context context) throws IOException {
		FileSystem fs = getFileSystem(context);
		Path path = toPath(context);
		if(fs.exists(path)) return fs.delete(path, true);
		return false;
	}

	@Override
	public List<Path> getPath(Context context, Object... arguments) throws IOException {
		Path path = toPath(context);
		if(arguments.length > 1 && arguments[0] instanceof Path) return Arrays.asList(new Path((Path)arguments[0], path)); 
		else return Arrays.asList(path);
	}

	@Override
	public FileSystem getFileSystem(Context context) throws IOException {
		return toPath(context).getFileSystem(context.get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) != null? (Configuration)context.get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) : new Configuration());
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj == null || !(obj instanceof FileDataFunction)) return false;
		else{
			FileDataFunction that = (FileDataFunction)obj;
			if(that.path.equals(path)) return true;			
		}
		return false;
	}
	
	private Path toPath(Context context) throws IOException{
		return Utils.resolvePath(Utils.replaceVariables(context, this.path), getWorkFolder());
	}
	
}
