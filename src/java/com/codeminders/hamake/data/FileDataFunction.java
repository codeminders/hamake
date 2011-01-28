package com.codeminders.hamake.data;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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
	public List<HamakePath> getHamakePath(Context context) throws IOException{		
		Path thisPath = getPath(context).get(0);
		if(thisPath.getParent() != null){
			return Arrays.asList(new HamakePath(thisPath.toString(), thisPath.getParent().toString()));
		}
		else{
			return Arrays.asList(new HamakePath(thisPath.toString(), thisPath.toString()));
		}
	}
	
	@Override
	public List<Path> getLocalPath(Context context) throws IOException {
		Path localPath;
		try {
			localPath = new Path(Utils.removeSchema(Utils.replaceVariables(context, this.path)));
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		return Arrays.asList(localPath);
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
		Path p = getPath(context).get(0);
		FileSystem fs = p.getFileSystem((Configuration)context.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
		if (!fs.exists(p))
			return 0;

		return Utils.recursiveGetModificationTime(fs, p);
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
