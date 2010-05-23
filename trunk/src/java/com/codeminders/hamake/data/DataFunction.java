package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.context.Context;

public abstract class DataFunction{

	private String id;
	private int generation;
	private long validityPeriod;
	private String workFolder;
	
	protected DataFunction(String id, int generation, long validityPeriod, String workFolder){
		this.id = id;
		this.generation = generation;
		this.validityPeriod = validityPeriod;
		this.workFolder = workFolder;
	}

	public abstract List<Path> getPath(Context context, Object... arguments)  throws IOException;
	
	public abstract boolean clear(Context context) throws IOException;
	
	public abstract FileSystem getFileSystem(Context context, Path path) throws IOException;
	
	public abstract long getMinTimeStamp(Context context) throws IOException;
	
	protected abstract String[] toString(Context context) throws IOException;
	
	public String getId(){
		return this.id;
	}
	
	public boolean isFile(Context context) throws IOException{
		List<Path> path = getPath(context);
		if(path.size() == 1){
			FileSystem fs = getFileSystem(context, path.get(0));
			if(fs.exists(path.get(0))){
				return !fs.getFileStatus(path.get(0)).isDir();
			}
		}
		return false;
	}
	
	public boolean isFolder(Context context) throws IOException{
		List<Path> path = getPath(context);
		if(path.size() == 1){
			FileSystem fs = getFileSystem(context, path.get(0));
			if(fs.exists(path.get(0))){
				return fs.getFileStatus(path.get(0)).isDir();
			}
		}
		return false;
	}
	
	public boolean isSet(Context context) throws IOException{
		return (getPath(context).size() > 1);
	}
	
	public boolean intersects(Context context, DataFunction that) throws IOException {
		for(String thisPath : this.toString(context)){
			for(String thatPath : that.toString(context)){
				boolean matches = false;
				matches = (FilenameUtils.wildcardMatch(thisPath, thatPath) || FilenameUtils.wildcardMatch(thatPath, thisPath))
				&& getGeneration() >= that.getGeneration();
				if(matches) return true;
			}
		}
		return false;
	}
	
	public int getGeneration() {
		return generation;
	}

	public long getValidityPeriod() {
		return validityPeriod;
	}

	public String getWorkFolder() {
		return workFolder;
	}
	
}
