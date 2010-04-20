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
	public void testFileDataFunction() throws IOException{
		Context context = new Context();
		File file1 = TestHelperUtils.generateTemporaryFile(tempDir.getAbsolutePath());
		File folder = TestHelperUtils.generateTemporaryDirectory(tempDir.getAbsolutePath());
		File file2 = TestHelperUtils.generateTemporaryFile(folder.getAbsolutePath());
		FileDataFunction fileFunc1 = new FileDataFunction(context, "1", 0, Long.MAX_VALUE, null, file1.getAbsolutePath());
		Assert.assertEquals(1, fileFunc1.getPath().size());
		Assert.assertEquals(file1.getAbsolutePath(), fileFunc1.getPath().get(0).toString());
		Assert.assertTrue(fileFunc1 + " should be a file", fileFunc1.isFile());
		FileDataFunction folderFunc = new FileDataFunction(context, "1", 0, Long.MAX_VALUE, null, folder.getAbsolutePath());
		Assert.assertEquals(1, folderFunc.getPath().size());
		Assert.assertEquals(folder.getAbsolutePath(), folderFunc.getPath().get(0).toString());
		Assert.assertTrue(folderFunc + " should be a folder", folderFunc.isFolder());
		Assert.assertTrue("FileDataFunction.getFileSystem should return an instance of LocalFileSystem", folderFunc.getFileSystem() instanceof LocalFileSystem);
		FileDataFunction fileFunc2 = new FileDataFunction(context, "2", 0, Long.MAX_VALUE, folder.getAbsolutePath(), FilenameUtils.getName(file2.getAbsolutePath()));
		Assert.assertEquals(1, fileFunc2.getPath().size());
		Assert.assertEquals(folder.getAbsolutePath() + File.separator + FilenameUtils.getName(file2.getAbsolutePath()), fileFunc2.getPath().get(0).toString());
		FileDataFunction fileFunc3 = new FileDataFunction(context, "2", 0, Long.MAX_VALUE, folder.getAbsolutePath(), file1.getAbsolutePath());
		Assert.assertEquals(1, fileFunc3.getPath().size());
		Assert.assertEquals(file1.getAbsolutePath(), fileFunc3.getPath().get(0).toString());
		FileDataFunction fileFunc4 = new FileDataFunction(context, "2", 0, Long.MAX_VALUE, null, file1.getAbsolutePath());
		Assert.assertTrue(fileFunc4.equals(fileFunc3));
		Assert.assertFalse(fileFunc4.equals(fileFunc2));
		Assert.assertTrue(folderFunc.intersects(fileFunc2));
		Assert.assertFalse(folderFunc.intersects(fileFunc3));
	}

}
