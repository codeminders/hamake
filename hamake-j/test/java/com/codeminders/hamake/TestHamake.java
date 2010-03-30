package com.codeminders.hamake;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.Permission;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.codeminders.hamake.commands.HadoopCommand;
import com.codeminders.hamake.tasks.MapTask;

public class TestHamake {

	private File tempDir;

	@After
	public void tearDown() {
		FileUtils.deleteQuietly(tempDir);
	}

	@Before
	public void setUp() {
		tempDir = TestHelperUtils.generateTemporaryDirectory();
	}

	 @Test
	 public void testLocalCpHamakefile() throws IOException,
	 ParserConfigurationException, SAXException,
	 InvalidMakefileException, InterruptedException {
	 // generate input and output folders folders
	 File tempInDir = TestHelperUtils.generateTemporaryDirectory(tempDir
	 .getAbsolutePath());
	 TestHelperUtils.generateTemporaryFiles(tempInDir.getAbsolutePath(), 10);
	 File tempMap1OutDir = TestHelperUtils
	 .generateTemporaryDirectory(tempDir.getAbsolutePath());
	 File tempMap2OutDir = TestHelperUtils
	 .generateTemporaryDirectory(tempDir.getAbsolutePath());
	 File tempReduce1OutDir = TestHelperUtils
	 .generateTemporaryDirectory(tempDir.getAbsolutePath());
	 File tempReduce1OutFile = TestHelperUtils
	 .generateTemporaryFile(tempReduce1OutDir.getAbsolutePath());
	 Thread.sleep(5000);
	 MakefileParser parser = new MakefileParser();
	 Hamake make = new Hamake();
	 File localHamakeFile = new File("hamakefile-local-cp.xml");
	 make = parser.parse(new FileInputStream(localHamakeFile), null, true);
	 if (OS.isLinux()) {
	 TestHelperUtils.setTaskExecBinary(make, "map1", "cp");
	 TestHelperUtils.setTaskExecBinary(make, "map2", "cp");
	 TestHelperUtils.setTaskExecBinary(make, "reduce", "ls");
	 } else if (OS.isWindows()) {
	 TestHelperUtils.setTaskExecBinary(make, "map1", "copy");
	 TestHelperUtils.setTaskExecBinary(make, "map2", "copy");
	 TestHelperUtils.setTaskExecBinary(make, "reduce", "dir");
	 }
	 TestHelperUtils.setMapTaskInputOutputFolders(make, "map1", new
	 HamakePath(
	 tempInDir.getAbsolutePath()), new HamakePath(tempMap1OutDir
	 .getAbsolutePath()));
	 TestHelperUtils.setMapTaskInputOutputFolders(make, "map2", new
	 HamakePath(
	 tempMap1OutDir.getAbsolutePath()), new HamakePath(tempMap2OutDir
	 .getAbsolutePath()));
	 TestHelperUtils.setReduceTaskInputOutputFolders(make, "reduce",
	 new HamakePath(tempMap2OutDir.getAbsolutePath()), new HamakePath(
	 tempReduce1OutFile.getAbsolutePath()));
	 make.setNumJobs(2);
	 make.run();
	 int map1Out = FileUtils.listFiles(tempMap1OutDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size();
	 int map2Out = FileUtils.listFiles(tempMap2OutDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size();
	 Assert.assertTrue("Amount of output files", map1Out > 8 && map1Out < 11);
	 Assert.assertTrue("Amount of output files", map2Out > 8 && map2Out < 11);
	 Assert.assertEquals(1, FileUtils.listFiles(tempReduce1OutDir,
	 TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
	 Assert.assertTrue("File size of output is 0 ",
	 FileUtils.sizeOfDirectory(tempReduce1OutDir) > 0);
	 }
	
	 @Test
	 public void test2BranchesLocalCpHamakefile() throws IOException,
	 ParserConfigurationException, SAXException,
	 InvalidMakefileException, InterruptedException {
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
	 .generateTemporaryFile(tempReduce2OutDir.getAbsolutePath());
	 Thread.sleep(5000);
	 MakefileParser parser = new MakefileParser();
	 Hamake make = new Hamake();
	 File localHamakeFile = new File("hamakefile-local-2-branches-cp.xml");
	 make = parser.parse(new FileInputStream(localHamakeFile), null, true);
	 if (OS.isLinux()) {
	 TestHelperUtils.setTaskExecBinary(make, "map11", "cp");
	 TestHelperUtils.setTaskExecBinary(make, "map12", "cp");
	 TestHelperUtils.setTaskExecBinary(make, "map21", "cp");
	 TestHelperUtils.setTaskExecBinary(make, "map22", "cp");
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
	 TestHelperUtils.setMapTaskInputOutputFolders(make, "map11", new
	 HamakePath(
	 tempInDir.getAbsolutePath()), new HamakePath(tempMap11OutDir
	 .getAbsolutePath()));
	 TestHelperUtils.setMapTaskInputOutputFolders(make, "map12", new
	 HamakePath(
	 tempInDir.getAbsolutePath()), new HamakePath(tempMap12OutDir
	 .getAbsolutePath()));
	 TestHelperUtils.setMapTaskInputOutputFolders(make, "map21", new
	 HamakePath(
	 tempMap11OutDir.getAbsolutePath()), new HamakePath(tempMap21OutDir
	 .getAbsolutePath()));
	 TestHelperUtils.setMapTaskInputOutputFolders(make, "map22", new
	 HamakePath(
	 tempMap12OutDir.getAbsolutePath()), new HamakePath(tempMap22OutDir
	 .getAbsolutePath()));
	 TestHelperUtils.setReduceTaskInputOutputFolders(make, "reduce1",
	 new HamakePath(tempMap21OutDir.getAbsolutePath()), new HamakePath(
	 tempReduce1OutFile.getAbsolutePath()));
	 TestHelperUtils.setReduceTaskInputOutputFolders(make, "reduce2",
	 new HamakePath(tempMap22OutDir.getAbsolutePath()), new HamakePath(
	 tempReduce2OutFile.getAbsolutePath()));
	 make.setNumJobs(2);
	 make.run();
	 int map11Out = FileUtils.listFiles(tempMap11OutDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size();
	 int map21Out = FileUtils.listFiles(tempMap21OutDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size();
	 int map22Out = FileUtils.listFiles(tempMap22OutDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size();
	 Assert.assertTrue("Amount of map11 output files", map11Out > 8 && map11Out < 11);
	 Assert.assertTrue("Amount of map11 output files", map21Out > 8 && map21Out < 11);
	 Assert.assertTrue("Amount of map11 output files", map22Out > 8 && map22Out < 11);
	 Assert.assertEquals(1, FileUtils.listFiles(tempReduce1OutDir,
	 TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
	 Assert.assertTrue("File size of output is 0 ",
	 FileUtils.sizeOfDirectory(tempReduce1OutDir) > 0);
	 Assert.assertEquals(1, FileUtils.listFiles(tempReduce2OutDir,
	 TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
	 Assert.assertTrue("File size of output is 0 ",
	 FileUtils.sizeOfDirectory(tempReduce2OutDir) > 0);
	 }

	@Test	
	public void testSystemExitIsProhibited() throws IOException,
			ParserConfigurationException, SAXException,
			InvalidMakefileException, InterruptedException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		SecurityManager securityManager = System.getSecurityManager();
		@SuppressWarnings(value = { "unused" })		
		SecurityManager manager = new SecurityManager(){			
			boolean exitCalled = false;
			@Override
	        public void checkPermission(Permission perm) { }

	        @Override
	        public void checkPermission(Permission perm, Object context) { }

	        @Override
	        public void checkExit(int status) {
	        	exitCalled = true;
	        	throw new SecurityException();
	        }
		};	
		System.setSecurityManager(manager);
		File tempInDir = TestHelperUtils.generateTemporaryDirectory(tempDir
				.getAbsolutePath());
		TestHelperUtils.generateTemporaryFiles(tempInDir.getAbsolutePath(), 1);
		File tempOutDir = TestHelperUtils.generateTemporaryDirectory(tempDir
				.getAbsolutePath());
		MakefileParser parser = new MakefileParser();
		File localHamakeFile = new File("hamakefile-testexit.xml");
		final Hamake make = parser.parse(new FileInputStream(localHamakeFile), null, true);
		make.setNumJobs(1);
		((HadoopCommand) ((MapTask) make.getTasks().get(0)).getCommand())
				.setJar(TestHelperUtils.getExamplesJar().getAbsolutePath());
		TestHelperUtils.setMapTaskInputOutputFolders(make, "map",
				new HamakePath(tempInDir.getAbsolutePath()), new HamakePath(
						tempOutDir.getAbsolutePath()));
		make.run();
		Assert.assertFalse("Hamake has passed System.exit()", manager.getClass().getDeclaredField("exitCalled").getBoolean(manager));
		System.setSecurityManager(securityManager);
	}

}
