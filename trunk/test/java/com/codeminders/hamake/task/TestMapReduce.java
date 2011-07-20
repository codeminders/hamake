package com.codeminders.hamake.task;

import java.io.*;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;
import org.xml.sax.SAXException;

import com.codeminders.hamake.*;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.syntax.BaseSyntaxParser;
import com.codeminders.hamake.syntax.InvalidMakefileException;

public class TestMapReduce {

	private File tempDir;

	@After
	public void tearDown() {
		FileUtils.deleteQuietly(tempDir);
	}

	@Before
	public void setUp() {
		tempDir = HelperUtils.generateTemporaryDirectory();
	}

	@Test
	public void testLocalClasspath() throws InvalidContextStateException,
			IOException, ParserConfigurationException, SAXException,
			InvalidMakefileException, PigNotFoundException {
		Context context = new Context(new Configuration(), null, false, false,
				false);
		context.set("test.jar", HelperUtils.getTestJar().getCanonicalPath());
		context.set("test.classpath", new File("testMapReduceLib")
				.getAbsoluteFile().toString());
		File localHamakeFile = HelperUtils
				.getHamakeTestResource("test-local-classpath.xml");
		final Hamake make = BaseSyntaxParser.parse(context,
				new FileInputStream(localHamakeFile));
		make.setNumJobs(1);
		Assert.assertEquals(Hamake.ExitCode.OK, make.run());
	}

//	@Test
//	public void testDFSdClasspath() throws InvalidContextStateException,
//			IOException, ParserConfigurationException, SAXException,
//			InvalidMakefileException, PigNotFoundException,
//			InterruptedException {
//		Configuration conf = new Configuration();
//		conf.set("fs.default.name", "hdfs://localhost:9000");
//		conf.set("fs.hdfs.impl", "com.codeminders.hamake.MockHDFs");
//		conf.set("mapred.job.tracker", "localhost:9001");
//		Context context = new Context(conf, null, false, false, false);
//		Configuration hamakeJobConf = new Configuration();
//		hamakeJobConf.set("fs.default.name", "hdfs://localhost:9000");
//		hamakeJobConf.set("fs.hdfs.impl", "com.codeminders.hamake.MockHDFs");
//		hamakeJobConf.set("mapred.job.tracker", "localhost:9001");
//		List<File> localClasspathJars = new ArrayList<File>();
//		MapReduce mapReduce = new MapReduce();
//		File lib1 = File.createTempFile("lib1", ".jar", tempDir);
//		File lib2 = File.createTempFile("lib2", ".jar", tempDir);
//		mapReduce.setClasspath(Arrays.asList(new FileDataFunction("hdfs://"
//				+ lib1.getCanonicalPath()), new FileDataFunction("hdfs://"
//				+ lib2.getCanonicalPath())));
//		mapReduce.processClassPath(new MockHDFs(), context, localClasspathJars,
//				hamakeJobConf);
//		Assert.assertEquals(2, localClasspathJars.size());
//		Assert.assertEquals(lib1.getCanonicalPath() + ":"
//				+ lib2.getCanonicalPath(), hamakeJobConf
//				.get("mapred.job.classpath.archives"));
//		Assert.assertEquals(MockHDFs.DEFAULT_URL + lib1.getCanonicalPath()
//				+ "," + MockHDFs.DEFAULT_URL + lib2.getCanonicalPath(),
//				hamakeJobConf.get("mapred.cache.archives"));
//	}
//
//	@Test
//	public void testS3Classpath() throws InvalidContextStateException,
//			IOException, ParserConfigurationException, SAXException,
//			InvalidMakefileException, PigNotFoundException,
//			InterruptedException {
//		Configuration conf = new Configuration();
//		conf.set("fs.default.name", "hdfs://localhost:9000");
//		conf.set("fs.hdfs.impl", "com.codeminders.hamake.MockHDFs");
//		conf.set("fs.s3.impl", "com.codeminders.hamake.MockS3");
//		conf.set("mapred.job.tracker", "localhost:9001");
//		Context context = new Context(conf, null, false, false, false);
//		Configuration hamakeJobConf = new Configuration();
//		hamakeJobConf.set("fs.default.name", "hdfs://localhost:9000");
//		hamakeJobConf.set("fs.hdfs.impl", "com.codeminders.hamake.MockHDFs");
//		hamakeJobConf.set("fs.s3.impl", "com.codeminders.hamake.MockS3");
//		hamakeJobConf.set("mapred.job.tracker", "localhost:9001");
//		List<File> localClasspathJars = new ArrayList<File>();
//		MapReduce mapReduce = new MapReduce();
//		File lib1 = File.createTempFile("lib1", ".jar", tempDir);
//		File lib2 = File.createTempFile("lib2", ".jar", tempDir);
//		mapReduce.setClasspath(Arrays.asList(new FileDataFunction("s3://"
//				+ lib1.getCanonicalPath()), new FileDataFunction("s3://"
//				+ lib2.getCanonicalPath())));
//		mapReduce.processClassPath(new MockHDFs(), context, localClasspathJars,
//				hamakeJobConf);
//		Assert.assertEquals(2, localClasspathJars.size());
//		Assert.assertEquals(conf.get("hadoop.tmp.dir") + "/" + lib1.getName()
//				+ ":" + conf.get("hadoop.tmp.dir") + "/" + lib2.getName(),
//				hamakeJobConf.get("mapred.job.classpath.archives"));
//		Assert.assertEquals(MockHDFs.DEFAULT_URL + conf.get("hadoop.tmp.dir")
//				+ "/" + lib1.getName() + "," + MockHDFs.DEFAULT_URL
//				+ conf.get("hadoop.tmp.dir") + "/" + lib2.getName(),
//				hamakeJobConf.get("mapred.cache.archives"));
//	}

}
