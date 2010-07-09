package com.codeminders.hamake.testjar;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DummyJobThatThrowsIOException extends Configured implements Tool {
	
	public static int calledTimes = 0;

	@Override
	public int run(String[] args) throws Exception {
		calledTimes++;
		if((calledTimes % 2) == 0){
			throw new IOException("Dummy Exception");
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DummyJobThatThrowsIOException(), args);
	}

}
