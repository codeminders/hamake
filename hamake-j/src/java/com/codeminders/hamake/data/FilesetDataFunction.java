package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.Utils;

public class FilesetDataFunction extends DataFunction {
	
	public enum Variant {
		LIST, MASK;

		public static Variant parseString(String variant) {
			if (StringUtils.isEmpty(variant))
				return null;
			if (variant.equalsIgnoreCase("mask"))
				return MASK;
			else
				return LIST;
		}
	}
	
	public static final Log LOG = LogFactory.getLog(FilesetDataFunction.class);
	
	private String path;
	private String mask;
	
	public FilesetDataFunction(Context context, String id, int generation, long validityPeriod, String workFolder, String path, String mask) throws IOException{
		super(context, id, generation, validityPeriod, workFolder);
		this.path = path;
		this.mask = mask;
	}

	@Override
	public boolean clear() throws IOException {
		Path path = toPath();
		if(getFileSystem().exists(path)) return getFileSystem().delete(path, true);
		return false;
	}

	@Override
	public FileSystem getFileSystem() throws IOException {
		Configuration conf = getContext().get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) != null? (Configuration)getContext().get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) : new Configuration();
		return toPath().getFileSystem(conf);
	}

	@Override
	public List<Path> getPath(Object... arguments) throws IOException {
		FileSystem fs = getFileSystem();
		Path path = toPath();
		List<Path> filesList = new ArrayList<Path>();
		Boolean create = false;
		Variant variant = Variant.LIST;
		if(arguments.length == 1 && arguments[0] instanceof Boolean) create = (Boolean)arguments[0];
		if(arguments.length == 1 && arguments[0] instanceof String) variant = Variant.parseString((String)arguments[0]);

		boolean exists = fs.exists(path);
		if (!exists) {
			if (create) {
				LOG.info("Creating " + path);
				fs.mkdirs(path);
			} else {
				LOG.error("Path " + this + " does not exist!");
			}
			return Collections.emptyList();
		}

		if(variant == Variant.LIST){
			FileStatus[] list = fs.listStatus(path);
	
			for (FileStatus s : list) {
				Path p = s.getPath();
				if (Utils.matches(p, mask))
					filesList.add(p);
			}
		}
		else if(variant == Variant.MASK){
			if(mask != null){
				return Arrays.asList(new Path(path.toString() + "/" + mask));
			}
		}
		
		return filesList;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj == null || !(obj instanceof FilesetDataFunction)) return false;
		else{
			FilesetDataFunction that = (FilesetDataFunction)obj;
			if(that.path.equals(path) && that.mask.equals(mask)) return true;			
		}
		return false;
	}
	
	private Path toPath() throws IOException{
		String processedPath = Utils.replaceVariables(getContext(), this.path);
		Path path = new Path(processedPath);
		if (!path.isAbsolute() && !StringUtils.isEmpty(getWorkFolder())) path = new Path(getWorkFolder(), processedPath);
		
		if (!getFileSystem().getFileStatus(path).isDir()) {
			throw new IOException("Path " + path + " should be a folder");
		}
		return path;
	}
	
}
