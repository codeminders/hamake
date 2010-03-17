package com.codeminders.hamake.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
public class JarListingFilter extends Configured implements Tool
{
    @SuppressWarnings("unchecked")
    public static void main(String[] args)
        throws Exception
    {
        int ret = ToolRunner.run(new JarListingFilter(), args);
        //System.exit(ret);
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

        Job job = new Job(config, "JarListingFilter");
        job.setJarByClass(JarListingFilter.class);

        //set the InputFormat of the job to our InputFormat
        job.setInputFormatClass(TextInputFormat.class);

        // the keys are words (strings)
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //use the defined mapper
        job.setMapperClass(MapClass.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapClass
        extends Mapper<LongWritable, Text, LongWritable, Text>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String[] words = StringUtils.split(value.toString(), '\\', '\t');
            if(words.length < 3)
                throw new IOException("Invalid input line format");

            LongWritable k = new LongWritable(Long.parseLong(words[1]));
            Text        v = new Text(words[0]);
            context.write(k, v);
        }
    }
}
