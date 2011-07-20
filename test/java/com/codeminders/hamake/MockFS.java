package com.codeminders.hamake;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public abstract class MockFS extends FileSystem{
	
	public Configuration getConf() {
	    return new Configuration();
	}

	@Override
	public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
			throws IOException {
		RawLocalFileSystem fs = new RawLocalFileSystem();
		fs.setConf(new Configuration());
		return fs.append(new Path(arg0.toUri().getPath()));
	}

	@Override
	public FSDataOutputStream create(Path arg0, FsPermission arg1,
			boolean arg2, int arg3, short arg4, long arg5, Progressable arg6)
			throws IOException {
		RawLocalFileSystem fs = new RawLocalFileSystem();
		fs.setConf(new Configuration());
		return fs.create(new Path(arg0.toUri().getPath()));
	}

	@Override
	public boolean delete(Path arg0) throws IOException {
		return delete(arg0, false);
	}

	@Override
	public boolean delete(Path arg0, boolean arg1) throws IOException {
		return FileUtils.deleteQuietly(new File(arg0.toUri().getPath()));
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
// TODO: make it portable for v0.18        
//		return new FileStatus(0, new File(path.toUri().getPath()).isDirectory(), 1, 64, Long.MAX_VALUE, Long.MAX_VALUE, null, "owner", "755", path);
        return null;
	}

	@Override
	public Path getWorkingDirectory() {
		return new Path("/user/" + "some").makeQualified(this);
	}

	@Override
	public FileStatus[] listStatus(Path arg0) throws IOException {
		return null;
	}

	@Override
	public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
		return false;
	}

	@Override
	public FSDataInputStream open(Path arg0, int arg1) throws IOException {
		RawLocalFileSystem fs = new RawLocalFileSystem();
		fs.setConf(new Configuration());
		return fs.open(new Path(arg0.toUri().getPath()));
	}

	@Override
	public boolean rename(Path arg0, Path arg1) throws IOException {
		return false;
	}

	@Override
	public void setWorkingDirectory(Path arg0) {

	}

	@Override
	public boolean exists(Path f) throws IOException {
		return new File(f.toUri().getPath()).exists();
	}

	@Override
	public void copyToLocalFile(Path src, Path dst) throws IOException {
		copyToLocalFile(false, src, dst);
	}

	@Override
	public void copyToLocalFile(boolean delSrc, Path src, Path dst)
			throws IOException {
		FileUtils.copyFile(new File(src.toUri().getPath()), new File(dst.toUri().getPath()));
	}
	
}
