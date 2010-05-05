package com.codeminders.hamake;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.jar.JarFile;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUtils {
	
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
	public void testRemoveManifestAttributes() throws IOException{
		File manifest = new File("testUtils.jar");
		File tempFile = File.createTempFile("manifest", ".jar", tempDir);
		FileUtils.copyFile(manifest, tempFile);
		JarFile jar = new JarFile(Utils.removeManifestAttributes(tempFile, Arrays.asList(new String[] {"Main-Class"})));
		Assert.assertNull(jar.getManifest().getMainAttributes().getValue("Main-Class"));
		Assert.assertFalse(tempFile.exists());
	}
}
