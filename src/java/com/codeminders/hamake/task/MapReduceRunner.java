package com.codeminders.hamake.task;

import java.io.File;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MapReduceRunner extends Configured implements Tool{
	
	private File[] additionalCPJars;
	
	public MapReduceRunner(File[] additionalCPJars){
		this.additionalCPJars = additionalCPJars;
	}

	public int run(String argv[]) throws Exception {
		int exitCode = -1;
		Configuration conf = getConf();
		try {
			Method setCommandLineConfigMethod = JobClient.class.getDeclaredMethod("setCommandLineConfig", Configuration.class);
			setCommandLineConfigMethod.setAccessible(true);
			setCommandLineConfigMethod.invoke(null, conf);
			try {
				RunJarThread.main(argv, additionalCPJars);
				exitCode = 0;
			} catch (Throwable th) {
				System.err.println(StringUtils.stringifyException(th));
			}
		} catch (RuntimeException re) {
			exitCode = -1;
			System.err.println(re.getLocalizedMessage());
		}
		return exitCode;
	}

	public static void main(String[] argv, File[] additionalCPJars) throws Exception {
		MapReduceRunner runner = new MapReduceRunner(additionalCPJars);
		int status = ToolRunner.run(runner, argv);
		System.exit(status);
	}
}
