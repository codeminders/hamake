package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;

public class SetDataFunction extends DataFunction {
	
	private Set<DataFunction> dataFunctions;
	private FileSystem fs;
	
	public SetDataFunction(Context context, String id) throws IOException{
		super(context, id, 0, Long.MAX_VALUE, null);
		dataFunctions = new HashSet<DataFunction>();
		Configuration conf = context.get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) != null? (Configuration)context.get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) : new Configuration();
		fs = FileSystem.get(conf);
	}

	@Override
	public boolean clear() throws IOException {
		boolean result = true;
		for(DataFunction func : dataFunctions){
			result = func.clear()? result : false;
		}
		return result;
	}

	@Override
	public FileSystem getFileSystem() {
		return fs;
	}

	@Override
	public List<Path> getPath(Object... arguments) throws IOException {
		List<Path> paths = new ArrayList<Path>();
		for(DataFunction func : dataFunctions){
			paths.addAll(func.getPath());
		}
		return paths;
	}
	
	public void addDataFunction(DataFunction func){
		dataFunctions.add(func);
	}

}
