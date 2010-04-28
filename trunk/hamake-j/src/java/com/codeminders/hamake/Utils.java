package com.codeminders.hamake;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URI;

public class Utils {
	
	public static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^\\}]+)\\}");
	public static final URI AmazonEMRPigJarURI = URI.create("s3://elasticmapreduce/libs/pig/0.3/pig-0.3-amzn.jar");

	public static final Log LOG = LogFactory.getLog(Utils.class);

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
            Utils.class.getClassLoader().loadClass("org.apache.pig.Main");
        } catch (ClassNotFoundException e) {
            return false;
        } catch (NoClassDefFoundError e) {
            return false;
        }

        return true;
    }

    public static boolean isAmazonEMRPigAvailable()
    {
        try {
            FileSystem fs = FileSystem.get(AmazonEMRPigJarURI, new Configuration());
            FileStatus stat;

            stat = fs.getFileStatus(new Path(AmazonEMRPigJarURI.toString()));
            if (stat != null && !stat.isDir())
                return true;
        } catch (Exception e) {

        }

        return false;
    }
    
    public static String replaceVariables(Context context, String value){
		Matcher matcher = VARIABLE_PATTERN.matcher(value);
		StringBuilder outputValue = new StringBuilder();
		int curPos = 0;
		while(matcher.find()){
			int start = matcher.start();
			int end = matcher.end();
			String variable = value.substring(start + 2, end - 1);
			outputValue.append(value.substring(curPos, start));
			if(!StringUtils.isEmpty(context.getString(variable))){
				outputValue.append(context.getString(variable));
			}
			else{
				outputValue.append("*");
			}
			curPos = end;
		}
		outputValue.append(value.substring(curPos, value.length()));
		return outputValue.toString();
	}
    
    public static Path resolvePath(String pathStr, String workFolder){
    	Path path = new Path(pathStr);
    	if (!path.isAbsolute() && !StringUtils.isEmpty(workFolder)) path = new Path(workFolder, path);
    	return path;
    }
    
}