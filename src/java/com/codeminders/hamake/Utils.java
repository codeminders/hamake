package com.codeminders.hamake;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Utils {
	
	public static final Log LOG = LogFactory.getLog(Utils.class);              

    public static Map<String, FileStatus> getFileList(HamakePath ipath)
            throws IOException {
        return getFileList(ipath, false, null);
    }

    public static Map<String, FileStatus> getFileList(HamakePath ipath, String mask)
            throws IOException {
        return getFileList(ipath, false, mask);
    }

    public static Map<String, FileStatus> getFileList(HamakePath ipath, boolean create)
            throws IOException {
        return getFileList(ipath, create, null);
    }

    public static Map<String, FileStatus> getFileList(HamakePath ipath,
                                                      boolean create,
                                                      String mask)
            throws IOException {
        if (Config.getInstance().test_mode)
        	LOG.info("Scanning " + ipath);

        boolean exists;

        synchronized (ipath.getFileSystem()) {
            exists = ipath.getFileSystem().exists(ipath.getPathName());
        }
        if (!exists) {
            if (create) {
                LOG.info("Creating " + ipath);
                synchronized (ipath.getFileSystem()) {
                	ipath.getFileSystem().mkdirs(ipath.getPathName());
                }
                return Collections.emptyMap();
            } else {
            	LOG.error("Path " + ipath + " does not exist!");
                return null;
            }
        }

        FileStatus stat;
        synchronized (ipath.getFileSystem()) {
            stat = ipath.getFileSystem().getFileStatus(ipath.getPathName());
        }
        if (!stat.isDir()) {
        	LOG.error("Path " + ipath + " must be dir!");
            return null;
        }

        FileStatus list[];
        synchronized (ipath.getFileSystem()) {
            list = ipath.getFileSystem().listStatus(ipath.getPathName());
        }

        Map<String, FileStatus> ret = new HashMap<String, FileStatus>();
        for (FileStatus s : list) {
            Path p = s.getPath();
            if (matches(p, mask))
                ret.put(p.getName(), s);
        }
        return ret;
    }

    public static String getenv(String name, String defaultValue) {
        String ret = System.getenv(name);
        return !StringUtils.isEmpty(ret) ? ret : defaultValue;
    }

    public static int execute(String command) {
        if (Config.getInstance().verbose)
        	LOG.info("Executing " + command);
        try {
            if (Config.getInstance().dryrun)
                return 0;
            else {
            	String[] cmd = null;
            	if(OS.isLinux()){
            		cmd = new String[] {"/bin/sh", "-c", command};
            	}
            	else if(OS.isWindows()){
            		cmd = new String[] {"cmd", "/C", command};
            	}
            	else{
            		cmd = new String[] {command};
            	}
            	return Runtime.getRuntime().exec(cmd).waitFor();
            }
        } catch (IOException ex) {
        	LOG.error(command + " execution failed, I/O error", ex);
        } catch (InterruptedException ex) {
        	LOG.error(command + " execution is interrupted", ex);
        } catch (Exception ex) {
        	LOG.error(command + " execution failed, internal error", ex);
        }
        return -1000;
    }

    public static File copyToTemporaryLocal(String path, FileSystem fs)
            throws IOException {
    	File srcFile = new File(path);
    	Path srcPath = new Path(path);    	    	      
    	if(srcFile.exists()){
    		return srcFile;
    	}
    	else if(fs.exists(srcPath)){
    		File dstFile = File.createTempFile("hamake", ".jar");
    		if (Config.getInstance().verbose) {
    			LOG.info("Downloading " + path + " to " + dstFile.getAbsolutePath());
            }
            fs.copyToLocalFile(srcPath, new Path(dstFile.getAbsolutePath()));
            dstFile.deleteOnExit();
            return dstFile;
    	}
        else
            throw new IOException("Path not found: " + path);
        
    }

    public static boolean matches(Path p, String mask) {
        String name = p.getName();
        return mask == null || FilenameUtils.wildcardMatch(name, mask);
    }

    public static boolean isPigAvailable()
    {
        try {
            Utils.class.getClassLoader().loadClass(org.apache.pig.Main.class.getCanonicalName());
        } catch (ClassNotFoundException e) {
            return false;
        } catch (NoClassDefFoundError e) {
            return false;
        }

        return true;
    }
    
}
