package com.codeminders.hamake.testjar;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DummyJobThatCallsSystemExit extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {		
		System.exit(1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DummyJobThatCallsSystemExit(), args);
	}

}
