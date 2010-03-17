package com.codeminders.hamake.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * User: Alexander Sova (bird@codeminders.com)
 * Date: Mar 1, 2010
 * Time: 3:43:30 PM
 */
public class JarListing extends Configured implements Tool
{

    /**
     * The main driver for word count map/reduce program. Invoke this method to
     * submit the map/reduce job.
     *
     * @param args command-line parameters
     *
     * @throws java.lang.Exception
     *           When there is communication problems with the job tracker.
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args)
        throws Exception
    {
        int ret = ToolRunner.run(new JarListing(), args);
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

        Job job = new Job(config, "JarListing");
        job.setJarByClass(JarListing.class);

        //set the InputFormat of the job to our InputFormat
        job.setInputFormatClass(JarInputFormat.class);

        // the keys are words (strings)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //use the defined mapper
        job.setMapperClass(MapClass.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapClass
        extends Mapper<NullWritable, JarEntryInfoWritable, Text, Text>
    {
        @Override
        protected void map(NullWritable key, JarEntryInfoWritable value, Context context)
            throws IOException, InterruptedException
        {
            if(value.getEntry().isDirectory())
                return;
            
            Text k = new Text(value.getEntry().getName());
            Text v = new Text("" + value.getEntry().getSize() + "\t" + value.getEntry().getTime());
            context.write(k, v);
        }
    }
    
    public static class JarInputFormat
        extends FileInputFormat<NullWritable, JarEntryInfoWritable>
    {
        @Override
        protected boolean isSplitable(JobContext context, Path filename)
        {
            return false;
        }

        @Override
        public RecordReader<NullWritable, JarEntryInfoWritable> createRecordReader(InputSplit inputSplit,
                                                                     TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException
        {
            return new RecordReader<NullWritable, JarEntryInfoWritable> ()
            {

                private JarInputStream is;
                private JarEntry       entry;
                private boolean        processed;

                @Override
                public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                    throws IOException, InterruptedException
                {
                    Path path = ((FileSplit)inputSplit).getPath();
                    FileSystem fs = path.getFileSystem(taskAttemptContext.getConfiguration());
                    is = new JarInputStream(fs.open(path));
                }

                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException
                {
                    entry = is.getNextJarEntry();
                    processed = (entry == null);
                    return !processed;
                }

                @Override
                public NullWritable getCurrentKey() throws IOException, InterruptedException
                {
                    return NullWritable.get();
                }

                @Override
                public JarEntryInfoWritable getCurrentValue() throws IOException, InterruptedException
                {
                    return new JarEntryInfoWritable(entry);
                }

                @Override
                public float getProgress() throws IOException, InterruptedException
                {
                    return processed ? 1 : 0;
                }

                @Override
                public void close() throws IOException
                {
                    is.close();
                }
            };
        }
    }

    public static class JarEntryInfoWritable implements Writable
    {
        private JarEntry entry;

        public JarEntry getEntry()
        {
            return entry;
        }

        public JarEntryInfoWritable(JarEntry entry)
        {
            this.entry = entry;
        }

        public void write(DataOutput dataOutput) throws IOException
        {
            dataOutput.writeUTF(entry.getName());
            dataOutput.writeLong(entry.getTime());
            dataOutput.writeLong(entry.getSize());

        }

        public void readFields(DataInput dataInput) throws IOException
        {
            entry = new JarEntry(dataInput.readUTF());
            
            entry.setTime(dataInput.readLong());
            entry.setSize(dataInput.readLong());
        }
    }
}
