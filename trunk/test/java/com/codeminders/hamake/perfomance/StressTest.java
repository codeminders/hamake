package com.codeminders.hamake.perfomance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class is a base class for all Hamake stress tests. To create your own stress test
 * create a class in package  com.codeminders.hamake.perfomance that extends StressTest class 
 * and override method start(Configuration conf, Path tempDir)
 * Run your stress tests by issuing ant stress-test command
 *
 */
public abstract class StressTest implements Runnable{
	
	private float loadFactor = 1.0F; //100%
	private Configuration conf;
	private Path tempDir;
	private Exception exception;
	
	/**
	 * Get load factor - an abstract measure of load 
	 * that should be provided by your stress test 
	 * @return positive number in range from 0.0 to 1.0
	 */
	public int getLoadFactorInPercentage(){
		return (int)loadFactor * 100;
	}

	/**
	 * Get load factor - an abstract measure of load
	 * that should be provided by your stress test 
	 * @return positive number in range from 0 to 100
	 */
	public float getLoadFactor(){
		return loadFactor;
	}
	
	public void setLoadFactor(float loadFactor){
		if(loadFactor > 0 && loadFactor < 1.0F){
			this.loadFactor = loadFactor;
		}
	}
	
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public void setTempDir(Path tempDir) {
		this.tempDir = tempDir;
	}
	
	public Exception getException() {
		return exception;
	}

	/**
	 * Generate N empty files
	 * @param fs instance of Hadoop FileSystem class
	 * @param parentDir directory where files should be generated 
	 * @param N amount of files
	 * @return array of Hadoop object Path which represents generated files 
	 * @throws IOException
	 */
	public static Path[] generateFiles(FileSystem fs, Path parentDir, int N) throws IOException{
		List<Path> paths = new ArrayList<Path>();
		for(int i = 0; i < N; i++){
			Path newFile = new Path(parentDir, UUID.randomUUID().toString());
			FSDataOutputStream out = fs.create(newFile);
			if(out != null) out.close();
			paths.add(newFile);
		}
		return paths.toArray(new Path[paths.size()]);
	}
	
	@Override
	public void run(){
		try {
			start(conf, tempDir);
		} catch (Exception e) {
			exception = e;
		}
	}
	
	/**
	 * Implement this method and put here all logic of our stress test
	 * @param conf Hadoop Configuration instance 
	 * @param tempDir directory where you can put all your temporary files
	 * @throws Exception
	 */
	protected abstract void start(Configuration conf, Path tempDir) throws Exception;
		
}
