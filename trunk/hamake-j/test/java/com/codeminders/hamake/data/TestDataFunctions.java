package com.codeminders.hamake.data;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.InvalidContextVariableException;
import com.codeminders.hamake.TestHelperUtils;

public class TestDataFunctions {
	
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
	public void testFileDataFunction() throws IOException, InvalidContextVariableException{
		Context context = new Context();
		File file1 = TestHelperUtils.generateTemporaryFile(tempDir.getAbsolutePath());
		File folder = TestHelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File file2 = TestHelperUtils.generateTemporaryFile(folder.getAbsolutePath());
		context.set("somepath", FilenameUtils.getFullPath(file2.getAbsolutePath()));
		FileDataFunction fileFunc1 = new FileDataFunction("1", 0, Long.MAX_VALUE, null, file1.getAbsolutePath());
		Assert.assertEquals(1, fileFunc1.getPath(context).size());
		Assert.assertEquals(file1.getAbsolutePath(), fileFunc1.getPath(context).get(0).toString());
		Assert.assertTrue(fileFunc1 + " should be a file", fileFunc1.isFile(context));
		FileDataFunction folderFunc = new FileDataFunction("1", 0, Long.MAX_VALUE, null, folder.getAbsolutePath());
		Assert.assertEquals(1, folderFunc.getPath(context).size());
		Assert.assertEquals(folder.getAbsolutePath(), folderFunc.getPath(context).get(0).toString());
		Assert.assertTrue(folderFunc + " should be a folder", folderFunc.isFolder(context));
		Assert.assertTrue("FileDataFunction.getFileSystem should return an instance of LocalFileSystem", folderFunc.getFileSystem(context) instanceof LocalFileSystem);
		FileDataFunction fileFunc2 = new FileDataFunction(null, 0, Long.MAX_VALUE, folder.getAbsolutePath(), FilenameUtils.getName(file2.getAbsolutePath()));
		Assert.assertEquals(1, fileFunc2.getPath(context).size());
		Assert.assertEquals(folder.getAbsolutePath() + File.separator + FilenameUtils.getName(file2.getAbsolutePath()), fileFunc2.getPath(context).get(0).toString());
		FileDataFunction fileFunc3 = new FileDataFunction(null, 0, Long.MAX_VALUE, folder.getAbsolutePath(), file1.getAbsolutePath());
		Assert.assertEquals(1, fileFunc3.getPath(context).size());
		Assert.assertEquals(file1.getAbsolutePath(), fileFunc3.getPath(context).get(0).toString());
		FileDataFunction fileFunc4 = new FileDataFunction(null, 0, Long.MAX_VALUE, null, file1.getAbsolutePath());
		Assert.assertTrue(fileFunc4.equals(fileFunc3));
		Assert.assertFalse(fileFunc4.equals(fileFunc2));
		FileDataFunction fileFunc5 = new FileDataFunction("2", 0, Long.MAX_VALUE, null, "${somepath}" + File.separator + FilenameUtils.getName(file2.getAbsolutePath()));
		Assert.assertEquals(1, fileFunc5.getPath(context).size());
		Assert.assertEquals(file2.getAbsolutePath(), fileFunc5.getPath(context).get(0).toString());
		Assert.assertTrue(folderFunc.intersects(context, fileFunc2));
		Assert.assertFalse(folderFunc.intersects(context, fileFunc3));
		Assert.assertTrue(fileFunc1.clear(context));
		Assert.assertTrue(fileFunc2.clear(context));
		Assert.assertFalse(fileFunc3.clear(context));
		Assert.assertFalse(fileFunc4.clear(context));
	}
	
	@Test
	public void testFilesetDataFunction() throws IOException{
		Context context = new Context();
		File folder1 = TestHelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		TestHelperUtils.generateTemporaryFiles(folder1.getAbsolutePath(), 10, ".txt");
		File folder2 = TestHelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		TestHelperUtils.generateTemporaryFiles(folder1.getAbsolutePath(), 10, ".jar");
		FilesetDataFunction fileset1 = new FilesetDataFunction("id", 0, Long.MAX_VALUE, null, folder1.getAbsolutePath(), "*.txt");
		Assert.assertEquals(10, fileset1.getPath(context).size());
		Assert.assertTrue(fileset1 + " should be a set", fileset1.isSet(context));
		Assert.assertTrue("FilesetDataFunction.getFileSystem should return an instance of LocalFileSystem", fileset1.getFileSystem(context) instanceof LocalFileSystem);
		FilesetDataFunction fileset2 = new FilesetDataFunction("id", 0, Long.MAX_VALUE, null, folder2.getAbsolutePath(), "*.jar");
		FilesetDataFunction fileset3 = new FilesetDataFunction("id", 0, Long.MAX_VALUE, null, folder1.getAbsolutePath(), "*.txt");
		Assert.assertFalse(fileset1.intersects(context, fileset2));
		Assert.assertTrue(fileset1.intersects(context, fileset3));
	}
	
	@Test
	public void testSetDataFunction() throws IOException{
		Context context = new Context();
		File set1 = TestHelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		TestHelperUtils.generateTemporaryFiles(set1.getAbsolutePath(), 10, ".txt");
		FilesetDataFunction filesetFunc = new FilesetDataFunction("id", 0, Long.MAX_VALUE, null, set1.getAbsolutePath(), "*.txt");
		File file1 = TestHelperUtils.generateTemporaryFile(tempDir.getAbsolutePath());
		FileDataFunction fileFunc = new FileDataFunction("1", 0, Long.MAX_VALUE, null, file1.getAbsolutePath());
		File folder1 = TestHelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
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

}
