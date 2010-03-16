package com.codeminders.hamake.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * User: Alexander Sova (bird@codeminders.com)
 */
public class ClassSizeHistogram extends Configured implements Tool
{
    private static final int BIN_SIZE = 1024; // exclusive
    
    @SuppressWarnings("unchecked")
    public static void main(String[] args)
        throws Exception
    {
        int ret = ToolRunner.run(new ClassSizeHistogram(), args);
        System.exit(ret);
    }

    private void printUsage()
    {
        System.out.println("Usage : " + getClass().getName() + " <input> <output>");
    }

    public int run(String[] args) throws Exception
    {
        Configuration config = new Configuration();

        if(args.length < 2)
        {
          printUsage();
          return 1;
        }

        Job job = new Job(config, "ClassSizeHistogram");
        job.setJarByClass(ClassSizeHistogram.class);

        // set the InputFormat of the job to our InputFormat
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // use the defined mapper
        job.setMapperClass(MapClass.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        Path p = new Path(args[0]);
        FileStatus[] list = p.getFileSystem(config).listStatus(p);
        for(FileStatus f : list)
            FileInputFormat.addInputPath(job, f.getPath());

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapClass
        extends Mapper<LongWritable, Text, LongWritable, IntWritable>
    {
        private              LongWritable histogramBin = new LongWritable();
        private final static IntWritable  one          = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String[] words = StringUtils.split(value.toString(), '\\', '\t');
            if(words.length < 2)
                throw new IOException("Invalid input line format");
            long size = Long.parseLong(words[0]);

            histogramBin.set(BIN_SIZE*((long)Math.floor(size/BIN_SIZE)+1));

            context.write(histogramBin, one);
        }
    }

    public static class IntSumReducer
         extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable>
    {
      private IntWritable result = new IntWritable();

      public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException
      {
        int sum = 0;
        for (IntWritable val : values)
          sum += val.get();

        result.set(sum);
        context.write(key, result);
      }
    }
}