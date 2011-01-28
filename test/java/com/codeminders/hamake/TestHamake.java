package com.codeminders.hamake;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
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
		File localHamakeFile = HelperUtils.getHamakeTestResource("test-local-cp.xml");
		make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		int map1OutSize = FileUtils.listFiles(map1Dir, FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")),
				null).size();
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
		File localHamakeFile = HelperUtils.getHamakeTestResource("test-that-foreach-launches-in-case-data-has-been-changed.xml");
		make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		Thread.sleep(1000);
		int map1OutSize = FileUtils.listFiles(map1Dir, FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")),
				null).size();
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
		File localHamakeFile = HelperUtils.getHamakeTestResource("test-2-branches-local-cp.xml");
		make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		int map21OutSize = FileUtils.listFiles(map21Dir,
				FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")), null).size();
		Assert.assertEquals(10, map21OutSize);
		int map22OutSize = FileUtils.listFiles(map22Dir,
				FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")), null).size();
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
		File localHamakeFile = HelperUtils.getHamakeTestResource("test-system-exit-is-prohibited.xml");
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

		File localHamakeFile = HelperUtils.getHamakeTestResource("test-dependencies.xml");
		final Hamake make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		HamakeRunner runner = new HamakeRunner(make);
		
		Thread th = new Thread(runner);
		th.start();
		Thread.sleep(2000);
		Assert.assertNull(runner.getException());
		int map1OutSize = FileUtils.listFiles(outputDir, FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")),
				null).size();
		Assert.assertEquals(0, map1OutSize);
		Assert.assertTrue(dependencyFile.createNewFile());
		Thread.sleep(2000);
		Assert.assertNull(runner.getException());
		map1OutSize = FileUtils.listFiles(outputDir, FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")),
				null).size();
		Assert.assertEquals(1, map1OutSize);
	}
	
	@Test
	public void testRefusedFiles() throws IOException, InvalidContextStateException, ParserConfigurationException, SAXException, InvalidMakefileException, PigNotFoundException, InterruptedException{
		File inputDir = new File(tempDir, "input");
		inputDir.mkdirs();
		HelperUtils.generateTemporaryFiles(inputDir.getCanonicalPath(), 10);
		File outputDir = new File(tempDir, "output");
		File refusedDir = new File(tempDir, "refused");
		outputDir.mkdirs();
		String tempDirPath = tempDir.getCanonicalPath();
		Context context = new Context(new Configuration(), null, true, false, false);
		context.set("tmpdir", tempDirPath);
		context.set("test.jar", HelperUtils.getTestJar().getCanonicalPath());

		File localHamakeFile = HelperUtils.getHamakeTestResource("test-refused-files.xml");
		final Hamake make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		Assert.assertTrue(refusedDir.exists());
		Assert.assertTrue(refusedDir.isDirectory());
		Collection<File> refusedFiles1 = FileUtils.listFiles(refusedDir, FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")),
				null);
		Assert.assertEquals("Amount of refused files", 5, refusedFiles1.size());
	}
	
	@Test
	public void testFailedTaskCausesDependentTasksToFail() throws IOException, InvalidContextStateException, ParserConfigurationException, SAXException, InvalidMakefileException, PigNotFoundException{
		File inputDir = new File(tempDir, "input");
		inputDir.mkdirs();
		HelperUtils.generateTemporaryFiles(inputDir.getAbsolutePath(), 4);
		File map1Dir = new File(tempDir, "map1");
		map1Dir.mkdirs();
		File outputFile = new File(tempDir, "output.txt");
		String tempDirPath = tempDir.getAbsolutePath().toString();
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("tmpdir", tempDirPath);
		context.set("test.jar", HelperUtils.getTestJar().getCanonicalPath());
		if (OS.isLinux()) {
			context.set("cp", "cp");
			context.set("ls", "ls");
		} else if (OS.isWindows()) {
			context.set("cp", "copy");
			context.set("ls", "dir");
		}

		Hamake make = null;
		File localHamakeFile = HelperUtils.getHamakeTestResource("test-failed-task-causes-dependent-tasks-to-fail.xml");
		make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		int map1OutSize = FileUtils.listFiles(map1Dir, FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")),
				null).size();
		Assert.assertEquals(0, map1OutSize);
		Assert.assertFalse(outputFile.exists());
	}
	
	@Test
	public void testFoldDoesNotClearsOutputFolderBeforeStart() throws IOException, InvalidContextStateException, ParserConfigurationException, SAXException, InvalidMakefileException, PigNotFoundException, InterruptedException{
		File outputDir = new File(tempDir, "output");
		outputDir.mkdirs();
		File someFileInOutputDir = new File(outputDir, "afile");
		someFileInOutputDir.createNewFile();
		Thread.sleep(1000);
		File inputFile = new File(tempDir, "input");
		inputFile.createNewFile();
		Assert.assertTrue(someFileInOutputDir.exists());
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("tmpdir", tempDir.getAbsolutePath().toString());
		if (OS.isLinux()) {
			context.set("cp", "cp");
			context.set("ls", "ls");
		} else if (OS.isWindows()) {
			context.set("cp", "copy");
			context.set("ls", "dir");
		}

		Hamake make = null;
		File localHamakeFile = HelperUtils.getHamakeTestResource("test_fold_does_not_clears_output_folder_before_start.xml");
		make = BaseSyntaxParser.parse(context, new FileInputStream(
				localHamakeFile));
		make.setNumJobs(2);
		make.run();
		Assert.assertTrue(outputDir.isDirectory());
		int amountOfFilesInOutputDir = FileUtils.listFiles(outputDir, FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".crc")),
				null).size();
		Assert.assertEquals(2, amountOfFilesInOutputDir);
		Assert.assertTrue(someFileInOutputDir.exists());
	}

}
