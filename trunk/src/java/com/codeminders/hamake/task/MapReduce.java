package com.codeminders.hamake.task;

import com.codeminders.hamake.Utils;
import com.codeminders.hamake.ExitException;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.params.Parameter;
import com.codeminders.hamake.params.SystemProperty;
import com.codeminders.hamake.params.JobConfParam;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapReduce extends Task {
    
	public static final Log LOG = LogFactory.getLog(MapReduce.class);
    private String jar;
    private String main;
    private List<DataFunction> classpath;

	public int execute(Context context) {
        FileSystem fs;
        List<String> args = new ArrayList<String>();
        List<File> classpathJars = new ArrayList<File>();
        File jarFile = null;
        Configuration hamakeJobConf = new Configuration();
        try {
            Path jarPath = new Path(getJar());             
            fs = jarPath.getFileSystem(new Configuration());
            jarFile = Utils.removeManifestAttributes(Utils.copyToTemporaryLocal(getJar(), fs), Arrays.asList(new String[] {"Main-Class"}));
            if(!classpath.isEmpty()){
            	Map<String, Boolean> alreadyHasList = new HashMap<String, Boolean>();
            	boolean isLocalMode = "local".equals(((Configuration)context.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION)).get("mapred.job.tracker", "local"));
            	if(!isLocalMode){
            		for(DataFunction func : classpath){
                    	for(Path cp : func.getPath(context)){
                    		if(fs.exists(cp)){
                    			File localJar = Utils.copyToTemporaryLocal(cp.toString(), fs);
                    			localJar.deleteOnExit();
                    			classpathJars.add(localJar);
                    			DistributedCache.addArchiveToClassPath(new Path(cp.toUri().getPath()), hamakeJobConf);
                    			alreadyHasList.put(cp.getName(), true);
                    		}
                    	}
                    }
                }
            	for(DataFunction func : classpath){
	            	for(Path cp : func.getLocalPath(context)){
	                	File localFile = new File(cp.toString());
	                	Path temp = new Path(((Configuration)context.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION)).get("hadoop.tmp.dir"));
	            		if(localFile.exists() && !alreadyHasList.containsKey(localFile.getName())){
	            			Path remoteJar = new Path(temp, localFile.getName());
	            			if(!isLocalMode){
		            			fs.copyFromLocalFile(new Path(localFile.getCanonicalPath()), remoteJar);
		            			fs.deleteOnExit(remoteJar);
		            			DistributedCache.addArchiveToClassPath(remoteJar, hamakeJobConf);
	            			}
	            			classpathJars.add(localFile);
	            		}
	                }
	            }
            	if(classpathJars.isEmpty()){
            		LOG.error("You have specified wrong classpath for " + getMain() + " task");
            		return -1000;
            	}
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
                        else if(p instanceof JobConfParam){
                        	hamakeJobConf.set(
                                ((JobConfParam)p).getName(),
                                ((JobConfParam)p).getValue());
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

            MapReduceRunner.main(s_args, classpathJars.toArray(new File[classpathJars.size()]), hamakeJobConf);
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
