package com.codeminders.hamake;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.codeminders.hamake.HelperUtils.HamakeRunner;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.syntax.BaseSyntaxParser;
import com.codeminders.hamake.syntax.InvalidMakefileException;

public class TestHamake {
	
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
	public void testLocalCpHamakefile() throws IOException,
			ParserConfigurationException, SAXException,
			InvalidMakefileException, InterruptedException,
			PigNotFoundException, InvalidContextStateException {

		File inputDir = new File(tempDir, "input");
		inputDir.mkdirs();
		HelperUtils.generateTemporaryFiles(inputDir.getAbsolutePath(), 10);
		File map1Dir = new File(tempDir, "map1");
		map1Dir.mkdirs();
		File outputFile = new File(tempDir, "output.txt");
		String tempDirPath = tempDir.getAbsolutePath().toString();
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("tmpdir", tempDirPath);
		if (OS.isLinux()) {
			context.set("cp", "cp");
			context.set("ls", "ls");
		} else if (OS.isWindows()) {
			context.set("cp", "copy");
			context.set("ls", "dir");
		}

		Hamake make = null;
		File localHamakeFile = HelperUtils.getHamakeTestResource("hamakefile-local-cp.xml");
		make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		int map1OutSize = FileUtils.listFiles(map1Dir, TrueFileFilter.INSTANCE,
				TrueFileFilter.INSTANCE).size();
		Assert.assertEquals(10, map1OutSize);
		Assert.assertTrue(outputFile.exists());
		Assert.assertTrue("File size of output is 0 ", outputFile.length() > 0);
	}
	
	@Test
	public void testThatForeachLaunchesInCaseDataHasBeenChanged() throws IOException,
			ParserConfigurationException, SAXException,
			InvalidMakefileException, InterruptedException,
			PigNotFoundException, InvalidContextStateException {

		File inputDir = new File(tempDir, "input");
		inputDir.mkdirs();
		File[] files = HelperUtils.generateTemporaryFiles(inputDir.getAbsolutePath(), 10);
		File map1Dir = new File(tempDir, "map1");
		map1Dir.mkdirs();
		String tempDirPath = tempDir.getAbsolutePath().toString();
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("tmpdir", tempDirPath);
		if (OS.isLinux()) {
			context.set("cp", "cp");
		} else if (OS.isWindows()) {
			context.set("cp", "copy");
		}

		Hamake make = null;
		File localHamakeFile = HelperUtils.getHamakeTestResource("hamakefile-foreach-test.xml");
		make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		Thread.sleep(1000);
		int map1OutSize = FileUtils.listFiles(map1Dir, TrueFileFilter.INSTANCE,
				TrueFileFilter.INSTANCE).size();
		Assert.assertEquals(10, map1OutSize);
		PrintWriter writer = new PrintWriter(files[0]);
		writer.println("someInfo");
		writer.close();
		make.run();
		File out = new File(map1Dir, files[0].getName());
		BufferedReader reader = new BufferedReader(new FileReader(out));
		String line = reader.readLine();
		reader.close();
		Assert.assertEquals("someInfo", line);
	}

	@Test
	public void test2BranchesLocalCpHamakefile() throws IOException,
			ParserConfigurationException, SAXException,
			InvalidMakefileException, InterruptedException,
			PigNotFoundException, InvalidContextStateException {

		File inputDir = new File(tempDir, "input");
		inputDir.mkdirs();
		HelperUtils.generateTemporaryFiles(inputDir.getAbsolutePath(), 10);
		File map11Dir = new File(tempDir, "map11");
		map11Dir.mkdirs();
		File map12Dir = new File(tempDir, "map12");
		map12Dir.mkdirs();
		File map21Dir = new File(tempDir, "map21");
		map21Dir.mkdirs();
		File map22Dir = new File(tempDir, "map22");
		map22Dir.mkdirs();
		File output1File = new File(tempDir, "output1.txt");
		File output2File = new File(tempDir, "output2.txt");
		String tempDirPath = tempDir.getAbsolutePath().toString();
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("tmpdir", tempDirPath);
		if (OS.isLinux()) {
			context.set("cp", "cp");
			context.set("ls", "ls");
		} else if (OS.isWindows()) {
			context.set("cp", "copy");
			context.set("ls", "dir");
		}

		Hamake make = null;
		File localHamakeFile = HelperUtils.getHamakeTestResource("hamakefile-local-2-branches-cp.xml");
		make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		int map21OutSize = FileUtils.listFiles(map21Dir,
				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size();
		Assert.assertEquals(10, map21OutSize);
		int map22OutSize = FileUtils.listFiles(map22Dir,
				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size();
		Assert.assertEquals(10, map22OutSize);
		Assert.assertTrue(output1File.exists());
		Assert
				.assertTrue("File size of output is 0 ",
						output1File.length() > 0);
		Assert.assertTrue(output2File.exists());
		Assert
				.assertTrue("File size of output is 0 ",
						output2File.length() > 0);
	}

	@Test
	public void testSystemExitIsProhibited() throws IOException,
			ParserConfigurationException, SAXException,
			InvalidMakefileException, InterruptedException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, PigNotFoundException,
			InvalidContextStateException {
		SecurityManager securityManager = System.getSecurityManager();
		ExitSecurityManager manager = new ExitSecurityManager();
		System.setSecurityManager(manager);
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("test.jar", HelperUtils.getTestJar()
				.getAbsolutePath());
		File localHamakeFile = HelperUtils.getHamakeTestResource("hamakefile-testexit.xml");
		final Hamake make = BaseSyntaxParser.parse(context,
				new FileInputStream(localHamakeFile));
		make.setNumJobs(1);
		make.run();
		Assert.assertFalse("Hamake has passed System.exit()", manager
				.getClass().getDeclaredField("exitCalled").getBoolean(manager));
		System.setSecurityManager(securityManager);
	}
	
	@Test
	public void testDependencies() throws IOException, InvalidContextStateException, ParserConfigurationException, SAXException, InvalidMakefileException, PigNotFoundException, InterruptedException{
		File inputDir = new File(tempDir, "input");
		inputDir.mkdirs();
		HelperUtils.generateTemporaryFiles(inputDir.getAbsolutePath(), 1);
		File outputDir = new File(tempDir, "output");
		outputDir.mkdirs();
		File dependencyFile = new File(tempDir, "dependency");
		String tempDirPath = tempDir.getAbsolutePath().toString();
		Context context = new Context(new Configuration(), null, true, false, false);
		context.set("tmpdir", tempDirPath);
		context.set("dependency.file", dependencyFile.getCanonicalPath());
		if (OS.isLinux()) {
			context.set("cp", "cp");
		} else if (OS.isWindows()) {
			context.set("cp", "copy");
		}

		File localHamakeFile = HelperUtils.getHamakeTestResource("hamakefile-test-dependencies.xml");
		final Hamake make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		HamakeRunner runner = new HamakeRunner(make);
		
		Thread th = new Thread(runner);
		th.start();
		Thread.sleep(2000);
		Assert.assertNull(runner.getException());
		int map1OutSize = FileUtils.listFiles(outputDir, TrueFileFilter.INSTANCE,
				TrueFileFilter.INSTANCE).size();
		Assert.assertEquals(0, map1OutSize);
		Assert.assertTrue(dependencyFile.createNewFile());
		Thread.sleep(2000);
		Assert.assertNull(runner.getException());
		map1OutSize = FileUtils.listFiles(outputDir, TrueFileFilter.INSTANCE,
				TrueFileFilter.INSTANCE).size();
		Assert.assertEquals(1, map1OutSize);
	}

}
