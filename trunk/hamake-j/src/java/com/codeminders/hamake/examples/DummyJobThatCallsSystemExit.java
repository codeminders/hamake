package com.codeminders.hamake.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DummyJobThatCallsSystemExit extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {		
		System.exit(1);
		return 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DummyJobThatCallsSystemExit(), args);
	}

}