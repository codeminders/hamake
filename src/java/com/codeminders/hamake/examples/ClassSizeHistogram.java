package com.codeminders.hamake.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: Alexander Sova (bird@codeminders.com)
 */
public class ClassSizeHistogram extends Configured implements Tool
{
    private static final int BIN_SIZE = 1024; // exclusive
    
    public static void main(String[] args)
        throws Exception
    {
        ToolRunner.run(new ClassSizeHistogram(), args);
    }

    private void printUsage()
    {
        System.out.println("Usage : " + getClass().getName() + " <input> <output>");
    }

    public int run(String[] args) throws Exception
    {
        JobConf config = new JobConf(ClassSizeHistogram.class);

        if(args.length < 2)
        {
          printUsage();
          return 1;
        }

        config.setJarByClass(ClassSizeHistogram.class);

        // set the InputFormat of the job to our InputFormat
        config.setInputFormat(TextInputFormat.class);

        config.setOutputKeyClass(LongWritable.class);
        config.setOutputValueClass(IntWritable.class);

        // use the defined mapper
        config.setMapperClass(MapClass.class);

        config.setCombinerClass(IntSumReducer.class);
        config.setReducerClass(IntSumReducer.class);

        config.setJobName("ClassSizeHistogram");

        Path p = new Path(args[0]);
        FileStatus[] list = p.getFileSystem(config).listStatus(p);
        for(FileStatus f : list)
            FileInputFormat.addInputPath(config, f.getPath());

        FileOutputFormat.setOutputPath(config, new Path(args[1]));


        return JobClient.runJob(config).isSuccessful() ? 0 : 1;
    }

    public static class MapClass
        implements Mapper<LongWritable, Text, LongWritable, IntWritable>
    {
        private              LongWritable histogramBin = new LongWritable();
        private final static IntWritable  one          = new IntWritable(1);

        public void map(LongWritable key, Text value, OutputCollector<LongWritable, IntWritable> outCollector, Reporter reporter) throws IOException {
            String[] words = StringUtils.split(value.toString(), '\\', '\t');
            if(words.length < 2)
                throw new IOException("Invalid input line format");
            long size = Long.parseLong(words[0]);

            histogramBin.set(BIN_SIZE*((long)Math.floor(size/BIN_SIZE)+1));

            outCollector.collect(histogramBin, one);
        }

        public void configure(JobConf jobConf) {
        }

        public void close() throws IOException {
        }
    }

    public static class IntSumReducer
            implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
      private IntWritable result = new IntWritable();

        public void reduce(
                LongWritable key,
                Iterator<IntWritable> values,
                OutputCollector<LongWritable, IntWritable> outCollector,
                Reporter reporter) throws IOException {
            int sum = 0;
            while(values.hasNext())
              sum += values.next().get();

            result.set(sum);
            outCollector.collect(key, result);

        }

        public void close() throws IOException {
        }

        public void configure(JobConf jobConf) {
        }
    }
}