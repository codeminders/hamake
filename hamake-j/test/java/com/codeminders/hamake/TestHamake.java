package com.codeminders.hamake;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.codeminders.hamake.commands.ExecCommand;
import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;
import com.codeminders.hamake.utils.TestHelperUtils;

public class TestHamake {

	private File tempDir = TestHelperUtils.generateTemporaryDirectory();

	@After
	public void tearDown() {
		FileUtils.deleteQuietly(tempDir);
	}

//	@Test
//	public void testLocalCpHamakefile() throws IOException,
//			ParserConfigurationException, SAXException,
//			InvalidMakefileException {
//		// generate input and output folders folders
//		File tempInDir = TestHelperUtils.generateTemporaryDirectory(tempDir
//				.getAbsolutePath());
//		TestHelperUtils.generateTemporaryFiles(tempInDir.getAbsolutePath(), 10);
//		File tempMap1OutDir = TestHelperUtils
//				.generateTemporaryDirectory(tempDir.getAbsolutePath());
//		File tempMap2OutDir = TestHelperUtils
//				.generateTemporaryDirectory(tempDir.getAbsolutePath());
//		File tempReduce1OutDir = TestHelperUtils
//				.generateTemporaryDirectory(tempDir.getAbsolutePath());
//		File tempReduce1OutFile = TestHelperUtils
//				.generateTemporaryFile(tempReduce1OutDir.getAbsolutePath());
//
//		MakefileParser parser = new MakefileParser();
//		Hamake make = new Hamake();
//		File localHamakeFile = new File("hamakefile-local-cp.xml");
//		make = parser.parse(new FileInputStream(localHamakeFile), true);
//		Collection<Task> tasks = make.getTasks();
//		Iterator<Task> i = tasks.iterator();
//		while (i.hasNext()) {
//			Task task = i.next();
//			if (task instanceof MapTask) {
//				MapTask m = (MapTask) task;
//				ExecCommand command = (ExecCommand) m.getCommand();
//				if (OS.isLinux()) {
//					command.setBinary("cp");
//				} else if (OS.isWindows()) {
//					command.setBinary("copy");
//				}
//				if (task.getName().equals("map1")) {
//					m.setXinput(new Path(tempInDir.getAbsolutePath()));
//					m.getOutputs().clear();
//					m.getOutputs().add(
//							new Path(tempMap1OutDir.getAbsolutePath()));
//				} else if (task.getName().equals("map2")) {
//					m.setXinput(new Path(tempMap1OutDir.getAbsolutePath()));
//					m.getOutputs().clear();
//					m.getOutputs().add(
//							new Path(tempMap2OutDir.getAbsolutePath()));
//				}
//			}
//			if (task instanceof ReduceTask) {
//				ReduceTask r = (ReduceTask) task;
//				if (task.getName().equals("reduce1")) {
//					Collection<Path> inputs = r.getInputs();
//					inputs.clear();
//					inputs.add(new Path(tempMap2OutDir.getAbsolutePath()));
//					r.setInputs(inputs);
//					r.getOutputs().clear();
//					r.getOutputs().add(
//							new Path(tempReduce1OutFile.getAbsolutePath()));
//					ExecCommand command = (ExecCommand) r.getCommand();
//					if (OS.isLinux()) {
//						command.setBinary("ls");
//					} else if (OS.isWindows()) {
//						command.setBinary("dir");
//					}
//				}
//			}
//		}
//		make.setFileSystem(FileSystem.get(new Configuration()));
//		make.setNumJobs(2);
//		make.run();
//		Assert.assertEquals(10, FileUtils.listFiles(tempMap1OutDir,
//				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
//		Assert.assertEquals(10, FileUtils.listFiles(tempMap2OutDir,
//				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
//		Assert.assertEquals(1, FileUtils.listFiles(tempReduce1OutDir,
//				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
//		Assert.assertTrue("File size of output is bigger then 0 ",
//				tempReduce1OutFile.length() > 0);
//	}

	@Test
	public void test2BranchesLocalCpHamakefile() throws IOException,
			ParserConfigurationException, SAXException,
			InvalidMakefileException {
		// generate input and output folders folders
		File tempInDir = TestHelperUtils.generateTemporaryDirectory(tempDir
				.getAbsolutePath());
		TestHelperUtils.generateTemporaryFiles(tempInDir.getAbsolutePath(), 10);
		File tempMap11OutDir = TestHelperUtils
				.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File tempMap12OutDir = TestHelperUtils
				.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File tempMap21OutDir = TestHelperUtils
				.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File tempMap22OutDir = TestHelperUtils
				.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File tempReduce1OutDir = TestHelperUtils
				.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File tempReduce1OutFile = TestHelperUtils
				.generateTemporaryFile(tempReduce1OutDir.getAbsolutePath());
		File tempReduce2OutDir = TestHelperUtils
				.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File tempReduce2OutFile = TestHelperUtils
				.generateTemporaryFile(tempReduce1OutDir.getAbsolutePath());

		MakefileParser parser = new MakefileParser();
		Hamake make = new Hamake();
		File localHamakeFile = new File("hamakefile-local-2-branches-cp.xml");
		make = parser.parse(new FileInputStream(localHamakeFile), true);
		if (OS.isLinux()) {
			TestHelperUtils.setTaskExecBinary(make, "map11", "cp");
			TestHelperUtils.setTaskExecBinary(make, "map12", "cp");
			TestHelperUtils.setTaskExecBinary(make, "map21", "cp");
			TestHelperUtils.setTaskExecBinary(make, "map21", "cp");
			TestHelperUtils.setTaskExecBinary(make, "reduce1", "ls");
			TestHelperUtils.setTaskExecBinary(make, "reduce2", "ls");
		} else if (OS.isWindows()) {
			TestHelperUtils.setTaskExecBinary(make, "map11", "copy");
			TestHelperUtils.setTaskExecBinary(make, "map12", "copy");
			TestHelperUtils.setTaskExecBinary(make, "map21", "copy");
			TestHelperUtils.setTaskExecBinary(make, "map21", "copy");
			TestHelperUtils.setTaskExecBinary(make, "reduce1", "dir");
			TestHelperUtils.setTaskExecBinary(make, "reduce2", "dir");
		}
		TestHelperUtils.setMapTaskInputOutputFolders(make, "map11", new Path(
				tempInDir.getAbsolutePath()), new Path(tempMap11OutDir
				.getAbsolutePath()));
		TestHelperUtils.setMapTaskInputOutputFolders(make, "map12", new Path(
				tempInDir.getAbsolutePath()), new Path(tempMap12OutDir
				.getAbsolutePath()));
		TestHelperUtils.setMapTaskInputOutputFolders(make, "map21", new Path(
				tempMap11OutDir.getAbsolutePath()), new Path(tempMap21OutDir
				.getAbsolutePath()));
		TestHelperUtils.setMapTaskInputOutputFolders(make, "map21", new Path(
				tempMap12OutDir.getAbsolutePath()), new Path(tempMap22OutDir
				.getAbsolutePath()));
		TestHelperUtils.setReduceTaskInputOutputFolders(make, "reduce1",
				new Path(tempMap21OutDir.getAbsolutePath()), new Path(
						tempReduce1OutFile.getAbsolutePath()));
		TestHelperUtils.setReduceTaskInputOutputFolders(make, "reduce2",
				new Path(tempMap22OutDir.getAbsolutePath()), new Path(
						tempReduce2OutFile.getAbsolutePath()));
		make.setFileSystem(FileSystem.get(new Configuration()));
		make.setNumJobs(2);
		make.run();
		Assert.assertEquals(10, FileUtils.listFiles(tempMap11OutDir,
				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
		Assert.assertEquals(10, FileUtils.listFiles(tempMap21OutDir,
				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
		Assert.assertEquals(10, FileUtils.listFiles(tempMap22OutDir,
				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
		Assert.assertEquals(1, FileUtils.listFiles(tempReduce1OutDir,
				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
		Assert.assertTrue("File size of output is bigger then 0 ",
				tempReduce1OutFile.length() > 0);
		Assert.assertEquals(1, FileUtils.listFiles(tempReduce2OutDir,
				TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
		Assert.assertTrue("File size of output is bigger then 0 ",
				tempReduce2OutFile.length() > 0);
	}
}
