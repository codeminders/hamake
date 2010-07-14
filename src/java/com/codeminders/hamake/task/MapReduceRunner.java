package com.codeminders.hamake.task;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.codeminders.hamake.ExitException;


public class MapReduceRunner extends Configured implements Tool{
	
	private File[] additionalCPJars;
	private Configuration additionalConfiguration;
	
	public MapReduceRunner(File[] additionalCPJars, Configuration additionalConfiguration){
		this.additionalCPJars = additionalCPJars;
		this.additionalConfiguration = additionalConfiguration;
	}

	public int run(String argv[]) throws Exception {
		try {
			RunJarThread.main(argv, additionalCPJars, additionalConfiguration);
		} catch(ExitException e){
			throw e;
		}
		catch (Throwable re) {
			throw new Exception(re);
		}
		return 0;
	}

	public static void main(String[] argv, File[] additionalCPJars, Configuration additionalConfiguration) throws Exception {
		MapReduceRunner runner = new MapReduceRunner(additionalCPJars, additionalConfiguration);
		ToolRunner.run(runner, argv);
	}
}
