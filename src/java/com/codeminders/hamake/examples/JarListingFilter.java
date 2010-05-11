package com.codeminders.hamake.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * User: Alexander Sova (bird@codeminders.com)
 */
public class JarListingFilter extends Configured implements Tool
{
    public static void main(String[] args)
        throws Exception
    {
        ToolRunner.run(new JarListingFilter(), args);
    }

    private void printUsage()
    {
        System.out.println("Usage : " + getClass().getName() + " <input> <output>");
    }

    public int run(String[] args) throws Exception
    {
        JobConf config = new JobConf(JarListingFilter.class);
        if(args.length < 2)
        {
          printUsage();
          return 1;
        }

        config.setJobName("JarListingFilter");
        config.setJarByClass(JarListingFilter.class);

        //set the InputFormat of the job to our InputFormat
        config.setInputFormat(TextInputFormat.class);

        // the keys are words (strings)
        config.setOutputKeyClass(LongWritable.class);
        config.setOutputValueClass(Text.class);

        //use the defined mapper
        config.setMapperClass(MapClass.class);

        FileInputFormat.addInputPaths(config, args[0]);
        FileOutputFormat.setOutputPath(config, new Path(args[1]));

        return JobClient.runJob(config).isSuccessful() ? 0 : 1;
    }

    public static class MapClass
            implements Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> outCollector, Reporter reporter) throws IOException {
            String[] words = StringUtils.split(value.toString(), '\\', '\t');
            if(words.length < 3)
                throw new IOException("Invalid input line format");

            LongWritable k = new LongWritable(Long.parseLong(words[1]));
            Text         v = new Text(words[0]);

            outCollector.collect(k, v);
        }

        public void configure(JobConf jobConf) {
        }

        public void close() throws IOException {
        }
    }
}
