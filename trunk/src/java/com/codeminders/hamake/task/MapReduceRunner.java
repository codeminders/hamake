package com.codeminders.hamake.task;

import java.io.File;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.codeminders.hamake.ExitException;


public class MapReduceRunner extends Configured implements Tool{
	
	private File[] additionalCPJars;
	
	public MapReduceRunner(File[] additionalCPJars){
		this.additionalCPJars = additionalCPJars;
	}

	public int run(String argv[]) throws Exception {
		Configuration conf = getConf();
		try {
			Method setCommandLineConfigMethod = JobClient.class.getDeclaredMethod("setCommandLineConfig", Configuration.class);
			setCommandLineConfigMethod.setAccessible(true);
			setCommandLineConfigMethod.invoke(null, conf);
			RunJarThread.main(argv, additionalCPJars);
		} catch(ExitException e){
			throw e;
		}
		catch (Throwable re) {
			throw new Exception(re);
		}
		return 0;
	}

	public static void main(String[] argv, File[] additionalCPJars) throws Exception {
		MapReduceRunner runner = new MapReduceRunner(additionalCPJars);
		ToolRunner.run(runner, argv);
	}
}
