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
	
	public SetDataFunction(String id) throws IOException{
		super(id, 0, Long.MAX_VALUE, null);
		dataFunctions = new HashSet<DataFunction>();
	}

	@Override
	public boolean clear(Context context) throws IOException {
		boolean result = true;
		for(DataFunction func : dataFunctions){
			result = func.clear(context)? result : false;
		}
		return result;
	}

	@Override
	public FileSystem getFileSystem(Context context) throws IOException {
		Configuration conf = context.get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) != null? (Configuration)context.get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) : new Configuration();
		return FileSystem.get(conf);
	}

	@Override
	public List<Path> getPath(Context context, Object... arguments) throws IOException {
		List<Path> paths = new ArrayList<Path>();
		for(DataFunction func : dataFunctions){
			paths.addAll(func.getPath(context));
		}
		return paths;
	}
	
	@Override
	public int getGeneration() {
		int generation = Integer.MIN_VALUE;
		for(DataFunction func : dataFunctions){
			generation  = (generation < func.getGeneration() ? func.getGeneration() : generation);
		}
		return generation;
	}

	@Override
	public long getValidityPeriod() {
		long validityPeriod = Long.MAX_VALUE;
		for(DataFunction func : dataFunctions){
			validityPeriod  = (validityPeriod > func.getValidityPeriod() ? func.getValidityPeriod() : validityPeriod);
		}
		return validityPeriod;
	}
	
	public void addDataFunction(DataFunction func){
		dataFunctions.add(func);
	}

}
