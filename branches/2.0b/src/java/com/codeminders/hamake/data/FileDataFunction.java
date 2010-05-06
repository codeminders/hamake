package com.codeminders.hamake.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Utils;

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
	public List<Path> getPath(Context context, Object... arguments)
			throws IOException {
		Path path = toPath(context);
		if (arguments.length > 1 && arguments[0] instanceof Path)
			return Arrays.asList(new Path((Path) arguments[0], path));
		else
			return Arrays.asList(path);
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
	public long getMinTimeStamp(Context context) throws IOException {
		FileSystem fs = getFileSystem(context, null);
		Path p = getPath(context).get(0);
		if (!fs.exists(p))
			return 0;

		FileStatus stat = fs.getFileStatus(p);

		return stat.getModificationTime();
	}
	
	@Override
	protected String[] toString(Context context) throws IOException{
		Path path = getPath(context).get(0);
		FileSystem fs = this.getFileSystem(context, path);
		return new String[] {fs.isFile(path)? path.getParent().toString() : path.toString()};
	}
	
	private Path toPath(Context context) throws IOException {
		return Utils.resolvePath(Utils.replaceVariables(context, this.path),
				getWorkFolder());
	}

}
