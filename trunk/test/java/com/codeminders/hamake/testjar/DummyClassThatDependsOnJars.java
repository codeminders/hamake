package com.codeminders.hamake.testjar;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.codeminders.helper.SomeHelper;
import com.codeminders.problem.Problem;

public class DummyClassThatDependsOnJars{
	
	public static class DummyClassThatDependsOnJarsMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{

		@Override
		public void map(Text arg0, Text arg1, OutputCollector<Text, Text> arg2,
				Reporter arg3) throws IOException {
			Problem.solve(SomeHelper.getHelp());
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(DummyClassThatDependsOnJars.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setMapperClass(DummyClassThatDependsOnJarsMapper.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		conf.setOutputFormat(NullOutputFormat.class);
		JobClient.runJob(conf);
	}
	
}
