package com.codeminders.hamake.perfomance;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.HelperUtils;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.syntax.BaseSyntaxParser;

public class HeapStressTest extends StressTest {

	@Override
	protected void start(Configuration conf, Path tempDir) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path inputDir = new Path(tempDir, "input");
		fs.mkdirs(inputDir);
		generateFiles(fs, inputDir, (int)(getLoadFactor() * 100000));
		Path outputDir = new Path(tempDir, "output");
		fs.mkdirs(outputDir);
		Context context = new Context(new Configuration(), null, false, false, false);
		if(!(fs instanceof LocalFileSystem)){
			Path dfsMapReduceJar = new Path("testMapReduce.jar");
			fs.copyFromLocalFile(new Path(HelperUtils.getHamakeTestResource("testMapReduce.jar").getCanonicalPath()), dfsMapReduceJar);
			context.set("test.jar", dfsMapReduceJar.toString());
		}
		else{
			context.set("test.jar", HelperUtils.getHamakeTestResource("testMapReduce.jar").getCanonicalPath());
		}
		
		context.set("input", inputDir.toString());
		context.set("output", outputDir.toString());
		File localHamakeFile = HelperUtils.getHamakeTestResource("stress-test-heap.xml");
		final Hamake make = BaseSyntaxParser.parse(context,
				new FileInputStream(localHamakeFile));
		if(!(fs instanceof LocalFileSystem)){
			make.setNumJobs(1000);
		}
		else{
			make.setNumJobs(1);
		}
		make.run();
	}

}
