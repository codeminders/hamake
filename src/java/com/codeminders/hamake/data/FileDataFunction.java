package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Utils;
import com.codeminders.hamake.context.Context;

public class FileDataFunction extends DataFunction {

	private String path;

	/**
	 * Constructor that is used in unit tests
	 * 
	 * @param path
	 *            location of a file or a folder
	 * @param generation
	 *            'generation' of DataFunction
	 * @throws IOException
	 */
	public FileDataFunction(String path, int generation) throws IOException {
		super(null, generation, Long.MAX_VALUE, null);
		this.path = path;
	}

	/**
	 * Constructor that is used in unit tests
	 * 
	 * @param path
	 *            location of a file or a folder
	 * @throws IOException
	 */
	public FileDataFunction(String path) throws IOException {
		super(null, 0, Long.MAX_VALUE, null);
		this.path = path;
	}

	public FileDataFunction(String id, int generation, long validityPeriod,
			String workFolder, String path) throws IOException {
		super(id, generation, validityPeriod, workFolder);
		this.path = path;
	}

	@Override
	public boolean clear(Context context) throws IOException {
		FileSystem fs = getFileSystem(context, null);
		Path path = toPath(context);
		if (fs.exists(path))
			return fs.delete(path, true);
		return false;
	}

	@Override
	public List<Path> getPath(Context context) {
		Path path = toPath(context);
		return Arrays.asList(path);
	}
	
	@Override 
	public List<Path> getParent(Context context) throws IOException{
		Path thisPath = getPath(context).get(0);
		if(thisPath.getParent() == null){
			return Arrays.asList(thisPath.getParent());
		}
		else return Arrays.asList(thisPath);
	}
	
	@Override
	public List<Path> getLocalPath(Context context) throws IOException {
		return Arrays.asList(new Path(Utils.replaceVariables(context, this.path)));
	}

	@Override
	public FileSystem getFileSystem(Context context, Path path)
			throws IOException {
		return toPath(context)
				.getFileSystem(
						context
								.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION) != null ? (Configuration) context
								.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION)
								: new Configuration());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof FileDataFunction))
			return false;
		else {
			FileDataFunction that = (FileDataFunction) obj;
			if (that.path.equals(path))
				return true;
		}
		return false;
	}

	@Override
	public long getMaxTimeStamp(Context context) throws IOException {
		FileSystem fs = getFileSystem(context, null);
		Path p = getPath(context).get(0);
		if (!fs.exists(p))
			return 0;

		FileStatus stat = fs.getFileStatus(p);

		return stat.getModificationTime();
	}
	
	@Override
	public boolean intersects(Context context, DataFunction that) throws IOException {
		if(that instanceof FileDataFunction){
			Path thisPath = getPath(context).get(0);
			Path thatPath = that.getPath(context).get(0);
			Path thisParent = (thisPath.getParent() == null)? thisPath : thisPath.getParent();
			Path thatParent = (thatPath.getParent() == null)? thatPath : thatPath.getParent();
			if(thisParent.equals(thatParent)){
				return thisPath.getName().equals(thatPath.getName()) && getGeneration() >= that.getGeneration();
			}
			if(thisPath.toUri().toString().length() > thatPath.toUri().toString().length()){
				return thisParent.toString().equals(thatPath.toString())
				&& getGeneration() >= that.getGeneration();
			}
			else{
				return thatParent.toString().equals(thisPath.toString())
				&& getGeneration() >= that.getGeneration();
			}
		}
		else return super.intersects(context, that);
	}
	
	@Override
	public String[] toString(Context context) {
		Path path = getPath(context).get(0);
		return new String[] {path.toString()};
	}
	
	private Path toPath(Context context) {
		return Utils.resolvePath(Utils.replaceVariables(context, this.path),
				getWorkFolder());
	}

}
