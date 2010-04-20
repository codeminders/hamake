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
	
	public FileDataFunction(Context context, String id, int generation, long validityPeriod, String workFolder, String path) throws IOException{
		super(context, id, generation, validityPeriod, workFolder);
		this.path = path;
	}

	@Override
	public boolean clear() throws IOException {
		FileSystem fs = getFileSystem();
		Path path = toPath();
		if(fs.exists(path)) return fs.delete(path, true);
		return false;
	}

	@Override
	public List<Path> getPath(Object... arguments) throws IOException {
		Path path = toPath();
		if(arguments.length > 1 && arguments[0] instanceof Path) return Arrays.asList(new Path((Path)arguments[0], path)); 
		else return Arrays.asList(path);
	}

	@Override
	public FileSystem getFileSystem() throws IOException {
		Configuration conf = getContext().get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) != null? (Configuration)getContext().get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) : new Configuration();
		return toPath().getFileSystem(conf);
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
	
	private Path toPath() throws IOException{
		String processedPath = Utils.replaceVariables(getContext(), this.path);
		Path path = new Path(processedPath);
		if (!path.isAbsolute() && !StringUtils.isEmpty(getWorkFolder())) path = new Path(getWorkFolder(), processedPath);
		
		return path;
	}
	
}
