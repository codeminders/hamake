package com.codeminders.hamake.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.*;
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
    public static void main(String[] args)
        throws Exception
    {
        ToolRunner.run(new JarListing(), args);        
    }

    private void printUsage()
    {
        System.out.println("Usage : " + getClass().getName() + " <input> <output>");
    }

    public int run(String[] args) throws Exception
    {
        JobConf config = new JobConf(JarListing.class);
        if(args.length < 2)
        {
          printUsage();
          return 1;
        }

        config.setJobName("JarListing");
        config.setJarByClass(JarListing.class);

        //set the InputFormat of the job to our InputFormat
        config.setInputFormat(JarInputFormat.class);

        // the keys are words (strings)
        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(Text.class);

        //use the defined mapper
        config.setMapperClass(MapClass.class);

        FileInputFormat.addInputPaths(config, args[0]);
        FileOutputFormat.setOutputPath(config, new Path(args[1]));

        return JobClient.runJob(config).isSuccessful() ? 0 : 1;
    }

    public static class MapClass
            implements Mapper<NullWritable, JarEntryInfoWritable, Text, Text> {

        public void map(NullWritable key, JarEntryInfoWritable value, OutputCollector<Text, Text> outCollector, Reporter reporter) throws IOException {
            if(value.getEntry().isDirectory())
                return;

            Text k = new Text(value.getEntry().getName());
            Text v = new Text("" + value.getEntry().getSize() + "\t" + value.getEntry().getTime());
            outCollector.collect(k, v);
        }

        public void configure(JobConf jobConf) {
        }

        public void close() throws IOException {
        }
    }
    
    public static class JarInputFormat
        extends FileInputFormat<NullWritable, JarEntryInfoWritable>
    {

        public RecordReader<NullWritable, JarEntryInfoWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            final JarInputStream is;

            Path path = ((FileSplit)inputSplit).getPath();

            FileSystem fs = path.getFileSystem(jobConf);
            is = new JarInputStream(fs.open(path));

            return new RecordReader<NullWritable, JarEntryInfoWritable> () {
                JarEntry       entry;
                boolean        processed;
                int            pos = 0;

                public boolean next(NullWritable key, JarEntryInfoWritable value) throws IOException {
                    entry = is.getNextJarEntry();
                    processed = (entry == null);
                    value.setEntry(entry);
                    return !processed;
                }

                public NullWritable createKey() {
                    return NullWritable.get();
                }

                public JarEntryInfoWritable createValue() {                    
                    return new JarEntryInfoWritable(null);
                }

                public long getPos() throws IOException {
                    return pos++;
                }

                public void close() throws IOException {
                    is.close();
                }

                public float getProgress() throws IOException {
                    return processed ? 1 : 0;
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

        public void setEntry(JarEntry entry) {
            this.entry = entry;
        }
    }
}
