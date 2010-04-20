package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Context;

public abstract class DataFunction {

	private Context context;
	private String id;
	private int generation;
	private long validityPeriod;
	private String workFolder;
	
	protected DataFunction(Context context, String id, int generation, long validityPeriod, String workFolder){
		this.id = id;
		this.generation = generation;
		this.validityPeriod = validityPeriod;
		this.workFolder = workFolder;
		this.context = context;
	}

	public abstract List<Path> getPath(Object... arguments)  throws IOException;
	
	public abstract boolean clear() throws IOException;
	
	public abstract FileSystem getFileSystem() throws IOException;
	
	public String getId(){
		return this.id;
	}
	
	public boolean isFile() throws IOException{
		if(getPath().size() == 1){
			return getFileSystem().isFile(getPath().get(0));
		}
		else return false;
	}
	
	public boolean isFolder() throws IOException{
		if(getPath().size() == 1){
			return !getFileSystem().isFile(getPath().get(0));
		}
		else return false;
	}
	
	public boolean isSet() throws IOException{
		return (getPath().size() > 1);
	}
	
	public boolean intersects(DataFunction that) throws IOException {
		for(Path thispath : this.getPath()){
			for(Path thatpath : that.getPath()){
				Path thisDir = getFileSystem().isFile(thispath)? thispath.getParent() : thispath;
				Path thatDir = getFileSystem().isFile(thatpath)? thatpath.getParent() : thatpath;
				String thisFileName = getFileSystem().isFile(thispath)? thispath.getName() : null;
				String thatFileName = getFileSystem().isFile(thatpath)? thatpath.getName() : null;
				boolean intersects = StringUtils.equals(thisDir.toString(), thatDir.toString())
				&& getGeneration() >= that.getGeneration()
				&& (thisFileName == null || thatFileName == null || StringUtils
						.equals(thisFileName, thatFileName));
				if(intersects) return true;
			}
		}
		return false;
	}

	protected int getGeneration() {
		return generation;
	}

	protected long getValidityPeriod() {
		return validityPeriod;
	}

	protected String getWorkFolder() {
		return workFolder;
	}
	
	protected Context getContext(){
		return context;
	}
	
	
	
}
