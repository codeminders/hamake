package com.codeminders.hamake.data;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.*;

import com.codeminders.hamake.HelperUtils;
import com.codeminders.hamake.InvalidContextStateException;
import com.codeminders.hamake.context.Context;

public class TestDataFunctions {
	
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
	public void testFileDataFunction() throws IOException, InvalidContextStateException{
		Context context = new Context(new Configuration(), null, false, false, false);
		File file1 = HelperUtils.generateTemporaryFile(tempDir.getAbsolutePath());
		File folder = HelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File file2 = HelperUtils.generateTemporaryFile(folder.getAbsolutePath());
		context.set("somepath", FilenameUtils.getFullPath(file2.getAbsolutePath()));
		FileDataFunction fileFunc1 = new FileDataFunction("1", 0, Long.MAX_VALUE, null, file1.getAbsolutePath());
		Assert.assertEquals(1, fileFunc1.getPath(context).size());
		Assert.assertEquals(new File(file1.getAbsolutePath()), new File(fileFunc1.getPath(context).get(0).toString()));
		Assert.assertTrue(fileFunc1 + " should be a file", fileFunc1.isFile(context));
		FileDataFunction folderFunc = new FileDataFunction("1", 0, Long.MAX_VALUE, null, folder.getAbsolutePath());
		Assert.assertEquals(1, folderFunc.getPath(context).size());
		Assert.assertEquals(new File(folder.getAbsolutePath()), new File(folderFunc.getPath(context).get(0).toString()));
		Assert.assertTrue(folderFunc + " should be a folder", folderFunc.isFolder(context));
		Assert.assertTrue("FileDataFunction.getFileSystem should return an instance of LocalFileSystem", folderFunc.getFileSystem(context, folderFunc.getPath(context).get(0)) instanceof LocalFileSystem);
		FileDataFunction fileFunc2 = new FileDataFunction(null, 0, Long.MAX_VALUE, folder.getAbsolutePath(), FilenameUtils.getName(file2.getAbsolutePath()));
		Assert.assertEquals(1, fileFunc2.getPath(context).size());
		Assert.assertEquals(new File(folder.getAbsolutePath() + File.separator + FilenameUtils.getName(file2.getAbsolutePath())),
                new File(fileFunc2.getPath(context).get(0).toString()));
		FileDataFunction fileFunc3 = new FileDataFunction(null, 0, Long.MAX_VALUE, folder.getAbsolutePath(), file1.getAbsolutePath());
		Assert.assertEquals(1, fileFunc3.getPath(context).size());
		Assert.assertEquals(
                new File(file1.getAbsolutePath()),
                new File(fileFunc3.getPath(context).get(0).toString()));
		FileDataFunction fileFunc4 = new FileDataFunction(null, 0, Long.MAX_VALUE, null, file1.getAbsolutePath());
		Assert.assertTrue(fileFunc4.equals(fileFunc3));
		Assert.assertFalse(fileFunc4.equals(fileFunc2));
		FileDataFunction fileFunc5 = new FileDataFunction("2", 0, Long.MAX_VALUE, null, "${somepath}" + File.separator + FilenameUtils.getName(file2.getAbsolutePath()));
		Assert.assertEquals(1, fileFunc5.getPath(context).size());
		Assert.assertEquals(
                new File(file2.getAbsolutePath()),
                new File(fileFunc5.getPath(context).get(0).toString()));
		Assert.assertTrue(folderFunc.intersects(context, fileFunc2));
		Assert.assertFalse(folderFunc.intersects(context, fileFunc3));
		Assert.assertTrue(fileFunc1.clear(context));
		Assert.assertTrue(fileFunc2.clear(context));
		Assert.assertFalse(fileFunc3.clear(context));
		Assert.assertFalse(fileFunc4.clear(context));
	}
	
	@Test
	public void testFilesetDataFunction() throws IOException, InvalidContextStateException{
		Context context = new Context(new Configuration(), null, false, false, false);
		File folder1 = HelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		HelperUtils.generateTemporaryFiles(folder1.getAbsolutePath(), 10, ".txt");
		File folder2 = HelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		HelperUtils.generateTemporaryFiles(folder1.getAbsolutePath(), 10, ".jar");
		FilesetDataFunction fileset1 = new FilesetDataFunction("id", 0, Long.MAX_VALUE, null, folder1.getAbsolutePath(), "*.txt");
		Assert.assertEquals(10, fileset1.getPath(context).size());
		Assert.assertTrue(fileset1 + " should be a set", fileset1.isSet(context));
		Assert.assertTrue("FilesetDataFunction.getFileSystem should return an instance of LocalFileSystem", fileset1.getFileSystem(context, fileset1.getPath(context).get(0)) instanceof LocalFileSystem);
		FilesetDataFunction fileset2 = new FilesetDataFunction("id", 0, Long.MAX_VALUE, null, folder2.getAbsolutePath(), "*.jar");
		FilesetDataFunction fileset3 = new FilesetDataFunction("id", 0, Long.MAX_VALUE, null, folder1.getAbsolutePath(), "*.txt");
		Assert.assertFalse(fileset1.intersects(context, fileset2));
		Assert.assertTrue(fileset1.intersects(context, fileset3));
	}
	
	@Test
	public void testSetDataFunction() throws IOException, InvalidContextStateException{
		Context context = new Context(new Configuration(), null, false, false, false);
		File set1 = HelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		HelperUtils.generateTemporaryFiles(set1.getAbsolutePath(), 10, ".txt");
		FilesetDataFunction filesetFunc = new FilesetDataFunction("id", 0, Long.MAX_VALUE, null, set1.getAbsolutePath(), "*.txt");
		File file1 = HelperUtils.generateTemporaryFile(tempDir.getAbsolutePath());
		FileDataFunction fileFunc = new FileDataFunction("1", 0, Long.MAX_VALUE, null, file1.getAbsolutePath());
		File folder1 = HelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		FileDataFunction folderFunc = new FileDataFunction("1", 0, Long.MAX_VALUE, null, folder1.getAbsolutePath());
		SetDataFunction setFunc = new SetDataFunction("id");
		SetDataFunction setSetFunc = new SetDataFunction("id2");
		setFunc.addDataFunction(filesetFunc);
		setFunc.addDataFunction(folderFunc);
		setFunc.addDataFunction(fileFunc);
		setSetFunc.addDataFunction(setFunc);
		Assert.assertEquals(12, setFunc.getPath(context).size());
		Assert.assertTrue(setFunc.intersects(context, filesetFunc));
		Assert.assertTrue(setFunc.intersects(context, setSetFunc));
	}
	
	@Test
	public void testIntersects() throws IOException, InvalidContextStateException{
		Context context = new Context(new Configuration(), null, false, false, false);
		FileDataFunction fileFuncA = new FileDataFunction("A", 0, Long.MAX_VALUE, null, "/tmp/test/${A}");
		FileDataFunction fileFuncB = new FileDataFunction("B", 0, Long.MAX_VALUE, null, "/tmp/test/${C}");
		Assert.assertTrue(fileFuncA.intersects(context, fileFuncB));
		Assert.assertTrue(fileFuncB.intersects(context, fileFuncA));
		File folder1 = HelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File file1 = HelperUtils.generateTemporaryFile(tempDir.getAbsolutePath());
		FileDataFunction fileFuncC = new FileDataFunction("C", 0, Long.MAX_VALUE, null, folder1.getAbsolutePath().toString());
		FileDataFunction fileFuncD = new FileDataFunction("D", 0, Long.MAX_VALUE, null, tempDir.getAbsolutePath().toString() + "/{D}/" + file1.getName());
		
		Assert.assertFalse(fileFuncC.intersects(context, fileFuncD));
		Assert.assertFalse(fileFuncD.intersects(context, fileFuncC));
		FileDataFunction fileFuncE = new FileDataFunction("/A");
		FileDataFunction fileFuncF = new FileDataFunction("/A/B");
		Assert.assertTrue(fileFuncF.intersects(context, fileFuncE));
		Assert.assertTrue(fileFuncE.intersects(context, fileFuncF));
		
		FileDataFunction fileFuncG = new FileDataFunction("/");
		FileDataFunction fileFuncH = new FileDataFunction("/");
		Assert.assertTrue(fileFuncH.intersects(context, fileFuncG));
		FileDataFunction fileFuncWG = new FileDataFunction("c:\\");
		FileDataFunction fileFuncWH = new FileDataFunction("c:\\");
		Assert.assertTrue(fileFuncWG.intersects(context, fileFuncWH));
		
		FileDataFunction fileFuncI = new FileDataFunction("/A/B/C");
		FileDataFunction fileFuncJ = new FileDataFunction("/A");
		Assert.assertFalse(fileFuncI.intersects(context, fileFuncJ));
		Assert.assertFalse(fileFuncJ.intersects(context, fileFuncI));
		
		FileDataFunction fileFuncK = new FileDataFunction("/A/B");
		FileDataFunction fileFuncL = new FileDataFunction("/A/G");
		Assert.assertFalse(fileFuncK.intersects(context, fileFuncL));
		Assert.assertFalse(fileFuncL.intersects(context, fileFuncK));
		
		FilesetDataFunction fileFuncM = new FilesetDataFunction(null, 0, Long.MAX_VALUE, null, "/A", "*.jpg");
		FileDataFunction fileFuncN = new FileDataFunction("/A/file.jpg");
		Assert.assertTrue(fileFuncN.intersects(context, fileFuncM));
		Assert.assertTrue(fileFuncM.intersects(context, fileFuncN));
		
		FileDataFunction fileFuncO = new FileDataFunction(null, 0, Long.MAX_VALUE, null, "somefolder/A/${foreach:filename}");
		FilesetDataFunction fileFuncP = new FilesetDataFunction(null, 0, Long.MAX_VALUE, null, "somefolder/A/", "*.log");
		Assert.assertTrue(fileFuncP.intersects(context, fileFuncO));
		Assert.assertTrue(fileFuncO.intersects(context, fileFuncP));
	}
	
	@Test
	public void testGetLocal() throws IOException{
		Context context = new Context(new Configuration(), null, false, false, false);
		FileDataFunction file = new FileDataFunction("hdfs://tmp/file");
		FileDataFunction localFile = new FileDataFunction("file:///tmp/file");
		FilesetDataFunction fileset = new FilesetDataFunction(null, 0, Long.MAX_VALUE, null, "s3://bucket/folder", "*.log");
		Assert.assertEquals(Collections.EMPTY_LIST, fileset.getLocalPath(context));
		Assert.assertEquals("[tmp/file]", file.getLocalPath(context).toString());
		Assert.assertEquals("[/tmp/file]", localFile.getLocalPath(context).toString());
	}
	
	@Test
	public void testGetParent() throws IOException{
		Context context = new Context(new Configuration(), null, false, false, false);
		FileDataFunction file = new FileDataFunction("s3://some_bucket");
		Assert.assertNotNull(file.getHamakePath(context).get(0));
		Assert.assertNotNull(file.getHamakePath(context).get(0).toString());
		FilesetDataFunction fileset = new FilesetDataFunction(null, 0, Long.MAX_VALUE, null, "s3://some_bucket", "*.log");
		Assert.assertNotNull(fileset.getHamakePath(context).get(0));
		Assert.assertNotNull(fileset.getHamakePath(context).get(0));
	}
	
}
