package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Context;

public abstract class DataFunction {

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
	
	public abstract FileSystem getFileSystem(Context context) throws IOException;
	
	public String getId(){
		return this.id;
	}
	
	public boolean isFile(Context context) throws IOException{
		if(getPath(context).size() == 1){
			return getFileSystem(context).isFile(getPath(context).get(0));
		}
		else return false;
	}
	
	public boolean isFolder(Context context) throws IOException{
		if(getPath(context).size() == 1){
			return !getFileSystem(context).isFile(getPath(context).get(0));
		}
		else return false;
	}
	
	public boolean isSet(Context context) throws IOException{
		return (getPath(context).size() > 1);
	}
	
	public boolean intersects(Context context, DataFunction that) throws IOException {
		for(Path thispath : this.getPath(context)){
			for(Path thatpath : that.getPath(context)){
				Path thisDir = getFileSystem(context).isFile(thispath)? thispath.getParent() : thispath;
				Path thatDir = getFileSystem(context).isFile(thatpath)? thatpath.getParent() : thatpath;
				String thisFileName = getFileSystem(context).isFile(thispath)? thispath.getName() : null;
				String thatFileName = getFileSystem(context).isFile(thatpath)? thatpath.getName() : null;
				boolean intersects = StringUtils.equals(thisDir.toString(), thatDir.toString())
				&& getGeneration() >= that.getGeneration()
				&& (thisFileName == null || thatFileName == null || StringUtils
						.equals(thisFileName, thatFileName));
				if(intersects) return true;
			}
		}
		return false;
	}
	
	public long getTimeStamp(Context context) throws IOException {
		FileSystem fs = getFileSystem(context);
		long modificationTime = Long.MIN_VALUE;
		for(Path p : getPath(context)){
			if (!fs.exists(p)) continue;
	
	        FileStatus stat = fs.getFileStatus(p);
	
	        if(stat.getModificationTime() > modificationTime) {
	        	modificationTime = stat.getModificationTime();
	        }
		}
		return modificationTime;
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
