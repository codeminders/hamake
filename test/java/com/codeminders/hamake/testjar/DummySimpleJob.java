package com.codeminders.hamake.testjar;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

public class DummySimpleJob {

	public static class DummySimpleJobMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{

		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> collector,
				Reporter reporter) throws IOException {
			collector.collect(key, value);
		}
	}
	
	public static class DummySimpleJobReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			output.collect(key, values.next());
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(DummySimpleJob.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setMapperClass(DummySimpleJobMapper.class);
		conf.setReducerClass(DummySimpleJobReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		conf.setOutputFormat(NullOutputFormat.class);
		JobClient.runJob(conf);
	}
	
}
