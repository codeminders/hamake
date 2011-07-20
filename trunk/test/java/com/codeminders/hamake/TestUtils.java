package com.codeminders.hamake;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.*;

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
	}
	
	@Test
	public void testCombineJars() throws IOException{
		File dir = new File("testUtilsLib");
		File mainJar = File.createTempFile("main", ".jar", tempDir);
		FileUtils.copyFile(new File("testUtils.jar"), mainJar);
		File combinedJar = Utils.combineJars(mainJar, dir);
		JarFile jar = new JarFile(combinedJar);
		Assert.assertNotNull(jar.getJarEntry("lib/testUtils1.jar"));
		Assert.assertNotNull(jar.getJarEntry("lib/testUtils2.jar"));
		Manifest man = jar.getManifest();
		Assert.assertEquals("org/apache/hadoop/examples/ExampleDriver", jar.getManifest().getMainAttributes().getValue("Main-Class"));
	}
	
}
