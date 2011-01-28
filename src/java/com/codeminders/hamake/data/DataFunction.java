package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.context.Context;

public abstract class DataFunction{
	
	public class HamakePath {
		
		private String path;		
		private String parent;
		
		public HamakePath(String filename, String folder){
			this.path = filename;
			this.parent = folder;
		}
		
		public String getPath() {
			return path;
		}
		public String getParent() {
			return parent;
		}

	}

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

	public abstract List<Path> getPath(Context context)  throws IOException;
	
	public abstract List<HamakePath> getHamakePath(Context context)  throws IOException;
	
	public abstract List<Path> getLocalPath(Context context)  throws IOException;
	
	public abstract boolean clear(Context context) throws IOException;
	
	public abstract FileSystem getFileSystem(Context context, Path path) throws IOException;
	
	public abstract long getMaxTimeStamp(Context context) throws IOException;
	
	public abstract String[] toString(Context context);

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
		for(HamakePath thisPath : this.getHamakePath(context)){
			for(HamakePath thatPath : that.getHamakePath(context)){
				if(getGeneration() >= that.getGeneration()){
					if(thisPath.getParent().equals(thatPath.getParent())){
						if((FilenameUtils.wildcardMatch(thisPath.getPath(), thatPath.getPath()) || FilenameUtils.wildcardMatch(thatPath.getPath(), thisPath.getPath()))) return true;
					}
					else if(FilenameUtils.wildcardMatch(thatPath.getPath(), thisPath.getParent())) return true;
					else if(FilenameUtils.wildcardMatch(thisPath.getParent(),thatPath.getPath())) return true;
					else if(FilenameUtils.wildcardMatch(thatPath.getParent(),thisPath.getPath())) return true;
					else if(FilenameUtils.wildcardMatch(thisPath.getPath(),thatPath.getParent())) return true;
				}
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
