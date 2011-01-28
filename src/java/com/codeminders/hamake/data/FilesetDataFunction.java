package com.codeminders.hamake.data;

import java.io.IOException;
import java.net.URISyntaxException;
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
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Utils;
import com.codeminders.hamake.context.Context;

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

	public FilesetDataFunction(String id, int generation, long validityPeriod,
			String workFolder, String path, String mask) throws IOException {
		super(id, generation, validityPeriod, workFolder);
		this.path = path;
		this.mask = mask;
	}

	@Override
	public boolean clear(Context context) throws IOException {
		Path path = toPath(context);
		if (getFileSystem(context, path).exists(path))
			return getFileSystem(context, path).delete(path, true);
		return false;
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
	public List<Path> getPath(Context context)
			throws IOException {

		Path path = toPath(context);
		FileSystem fs = getFileSystem(context, path);
		if (fs.exists(path) && !fs.getFileStatus(path).isDir()) {
			throw new IOException("Folder " + path	+ " should be a folder");
		}
		return listFiles(context, fs, path);
	}
	
	@Override 
	public List<HamakePath> getHamakePath(Context context) throws IOException{
		Path thisPath = toPath(context);
		return Arrays.asList(new HamakePath(toString(context)[0], thisPath.toString()));
	}
	
	@Override
	public List<Path> getLocalPath(Context context) throws IOException {
		Path localPath;
		try {
			localPath = new Path(Utils.removeSchema(Utils.replaceVariables(context, this.path)));
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		FileSystem localFs = FileSystem.getLocal((Configuration)context.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
		if(localFs.exists(new Path(localPath.toUri().getPath())) && !localFs.getFileStatus(localPath).isDir()){
			throw new IOException("Folder " + path + " should be a folder"); 
		}
		return listFiles(context, localFs, localPath);
	}
	
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof FilesetDataFunction))
			return false;
		else {
			FilesetDataFunction that = (FilesetDataFunction) obj;
			if (that.path.equals(path) && that.mask.equals(mask))
				return true;
		}
		return false;
	}

	@Override
	public long getMaxTimeStamp(Context context) throws IOException {
		long modificationTime = 0;
		FileSystem fs = null;
		for (Path p : getPath(context)) {
			if(fs == null)fs = p.getFileSystem((Configuration)context.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
			if (!fs.exists(p))
				continue;

			long mt = Utils.recursiveGetModificationTime(fs, p);

			if (mt > modificationTime) {
				modificationTime = mt;
			}
		}
		return modificationTime;
	}

	@Override
	public String[] toString(Context context){
		String str = Utils.replaceVariables(context, this.path);
		str += (str.endsWith("/") || str.endsWith("\\"))? (mask.equals("*") ? "" : mask) : "/" +(mask.equals("*") ? "" : mask);
		return new String[] {Utils.resolvePath(str, getWorkFolder()).toString()};
	}
	
	private Path toPath(Context context) throws IOException {
		return Utils.resolvePath(Utils.replaceVariables(context, this.path),
				getWorkFolder());
	}
	
	private List<Path> listFiles(Context context, FileSystem fs, Path folder) throws IOException{
		List<Path> filesList = new ArrayList<Path>();
		
		boolean exists = fs.exists(folder);
		if (!exists) {
			LOG.info("Creating " + folder);
			fs.mkdirs(folder);
			return Collections.emptyList();
		}

		FileStatus[] list = fs.listStatus(folder);

		for (FileStatus s : list) {
			Path p = ((fs instanceof LocalFileSystem? new Path(s.getPath().toUri().getPath()) : new Path(s.getPath().toString())));
			if (Utils.matches(p, mask))
				filesList.add(p);
		}

		return filesList;
	}

}
