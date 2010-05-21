package com.codeminders.hamake.task;

import com.codeminders.hamake.Utils;
import com.codeminders.hamake.ExitException;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.params.Parameter;
import com.codeminders.hamake.params.SystemProperty;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.RunJar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapReduce extends Task {
    
	public static final Log LOG = LogFactory.getLog(MapReduce.class);
    private String jar;
    private String main;
    private List<DataFunction> classpath;

	public int execute(Context context) {
        FileSystem fs;
        List<String> args = new ArrayList<String>();
        try {
            Path jarPath = new Path(getJar());             
            fs = jarPath.getFileSystem(new Configuration());
            File jarFile = Utils.removeManifestAttributes(Utils.copyToTemporaryLocal(getJar(), fs), Arrays.asList(new String[] {"Main-Class"}));
            if(!classpath.isEmpty()){
            	File tempDir = File.createTempFile(FilenameUtils.getBaseName(jar), "_classpath");
        		if(tempDir.exists())FileUtils.deleteQuietly(tempDir);
        		if(!tempDir.mkdirs()){
        			throw new IOException("can not create folder " + tempDir.getAbsolutePath());
        		}
            	for(DataFunction func : classpath){
            		for(Path cp : func.getPath(context)){
            			File copied = Utils.copyToTemporaryLocal(cp.toUri().getPath().toString(), fs);
            			FileUtils.moveFileToDirectory(copied, tempDir, true);
            		}
            	}
            	jarFile = Utils.combineJars(jarFile, tempDir);
            	FileUtils.deleteQuietly(tempDir);
            }
            args.add(jarFile.getAbsolutePath());
        } catch (IOException ex) {
        	LOG.error("Can't download JAR file: " + getJar(), ex);
            return -1000;
        }
        args.add(getMain());
        List<Parameter> params = getParameters();
        if (params != null) {
            for (Parameter p : params) {
                    try {
                    	if(p instanceof SystemProperty){
                        	System.setProperty(((SystemProperty)p).getName(), ((SystemProperty)p).getValue());
                        }
                    	else{
                    		args.add(p.get(context));
                    	}
                    } catch (IOException ex) {
                    	LOG.error("Failed to extract parameter values from parameter", ex);
                        return -1000;
                    }
            }
        }
        try {
            String s_args[] = new String[args.size()];
            args.toArray(s_args);
            if (context.getBoolean(Context.HAMAKE_PROPERTY_VERBOSE))
            	LOG.info("Executing Hadoop task " + StringUtils.join(s_args, ' '));
            if (context.getBoolean(Context.HAMAKE_PROPERTY_DRY_RUN))
                return 0;
            RunJar.main(s_args);            
        } catch (ExitException e){
            return e.status;
        } catch (Throwable ex) {
        	LOG.error("Failed to execute Hadoop command " + getJar() + '/' + getMain(), ex);
            return -1000;
        }
        return 0;
    }

    public String getJar() {
        return jar;
    }

    public void setJar(String jar) {
        this.jar = jar;
    }

    public String getMain() {
        return main;
    }

    public void setMain(String main) {
        this.main = main;
    }
    
    public List<DataFunction> getClasspath() {
		return classpath;
	}

	public void setClasspath(List<DataFunction> classpath) {
		this.classpath = classpath;
	}

	@Override
    public String toString() {
        return new ToStringBuilder(this).append("jar", jar).
                append("main", main).appendSuper(super.toString()).toString();
    }

}