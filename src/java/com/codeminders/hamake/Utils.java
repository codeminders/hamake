package com.codeminders.hamake;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.jar.JarArchiveEntry;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.conf.Configuration;

import com.codeminders.hamake.context.Context;

import java.io.*;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URI;

public class Utils {

	public static final Pattern VARIABLE_PATTERN = Pattern
			.compile("\\$\\{([^\\}]+)\\}");
	public static final URI AmazonEMRPigJarURI = URI
			.create("s3://elasticmapreduce/libs/pig/0.3/pig-0.3-amzn.jar");
	public static final String CloudEraPigDirPath = "/usr/lib/pig";

	public static final Log LOG = LogFactory.getLog(Utils.class);

	public static String getenv(String name, String defaultValue) {
		String ret = System.getenv(name);
		return !StringUtils.isEmpty(ret) ? ret : defaultValue;
	}

	public static int execute(Context context, String command) {
		if (context.getBoolean(Context.HAMAKE_PROPERTY_VERBOSE))
			LOG.info("Executing " + command);
		try {
			if (context.getBoolean(Context.HAMAKE_PROPERTY_DRY_RUN))
				return 0;
			else {
				String[] cmd = null;
				if (OS.isLinux()) {
					cmd = new String[] { "/bin/sh", "-c", command };
				} else if (OS.isWindows()) {
					cmd = new String[] { "cmd", "/C", command };
				} else {
					cmd = new String[] { command };
				}
				return Runtime.getRuntime().exec(cmd).waitFor();
			}
		} catch (IOException ex) {
			LOG.error(command + " execution failed, I/O error", ex);
		} catch (InterruptedException ex) {
			LOG.error(command + " execution is interrupted", ex);
		} catch (Exception ex) {
			LOG.error(command + " execution failed, internal error", ex);
		}
		return -1000;
	}

	public static File copyToTemporaryLocal(String path, FileSystem fs)
			throws IOException {
		File srcFile = new File(path);
		Path srcPath = new Path(path);
		File dstFile = File.createTempFile("hamake", ".jar");
		if (srcFile.exists()) {
			FileUtils.copyFile(srcFile, dstFile);
		} else if (fs.exists(srcPath)) {
			LOG.info("Downloading " + path + " to "
						+ dstFile.getAbsolutePath());
			fs.copyToLocalFile(srcPath, new Path(dstFile.getAbsolutePath()));
			dstFile.deleteOnExit();
		} else
			throw new IOException("Path not found: " + path);
		return dstFile;

	}

	public static File removeManifestAttributes(File jarFile,
			List<String> attributesToRemove) throws IOException {
		JarFile jar = new JarFile(jarFile);
		Attributes attributes = jar.getManifest().getMainAttributes();
		for (String attributeToRemove : attributesToRemove) {
			if (!StringUtils.isEmpty(attributes.getValue(attributeToRemove))) {
				File outputFile = File.createTempFile("hamake-", ".jar",
						jarFile.getParentFile());
				JarArchiveOutputStream jarOutputStream = new JarArchiveOutputStream(
						new FileOutputStream(outputFile));
				try {
					Enumeration<JarEntry> entries = jar.entries();
					while (entries.hasMoreElements()) {
						JarEntry entry = (JarEntry) entries.nextElement();
						if (entry.getName().equalsIgnoreCase(
								JarFile.MANIFEST_NAME)) {
							File tempManifest = new File(jarFile
									.getParentFile()
									+ File.separator
									+ jarFile.getName()
									+ "-unpacked", JarFile.MANIFEST_NAME);
							FileUtils.deleteQuietly(tempManifest);
							tempManifest.getParentFile().mkdirs();
							tempManifest.createNewFile();
							tempManifest.deleteOnExit();
							BufferedReader reader = null;
							PrintWriter writer = null;
							try {
								reader = new BufferedReader(
										new InputStreamReader(jar
												.getInputStream(entry)));
								writer = new PrintWriter(tempManifest);
								String line = null;
								while ((line = reader.readLine()) != null) {
									String[] nameValuePair = StringUtils.split(
											line, ":");
									if (nameValuePair.length == 2) {
										if (!StringUtils.equalsIgnoreCase(
												nameValuePair[0],
												attributeToRemove)) {
											writer.println(line);
										}
									}
								}
							} finally {
								if (writer != null)
									writer.close();
								if (reader != null)
									reader.close();
							}
							ArchiveEntry newManifestArchiveEntry = jarOutputStream
									.createArchiveEntry(tempManifest,
											JarFile.MANIFEST_NAME);
							jarOutputStream
									.putArchiveEntry(newManifestArchiveEntry);
							jarOutputStream.write(FileUtils
									.readFileToByteArray(tempManifest));
							jarOutputStream.closeArchiveEntry();
						} else {
							jarOutputStream
									.putArchiveEntry(new JarArchiveEntry(entry));
							if (!entry.isDirectory()) {
								InputStream in = null;
								try {
									byte[] buffer = new byte[8192];
									in = jar.getInputStream(entry);
									int i;
									while ((i = in.read(buffer)) != -1) {
										jarOutputStream.write(buffer, 0, i);
									}
									in.close();
									jarOutputStream.closeArchiveEntry();
								} finally {
									if (in != null)
										in.close();
								}
							}
						}
					}
				} finally {
					jar.close();
					jarOutputStream.close();
				}
				FileUtils.deleteQuietly(jarFile);
				outputFile.deleteOnExit();
				return outputFile;
			}
		}
		return jarFile;
	}

	public static boolean matches(Path p, String mask) {
		String name = p.getName();
		return mask == null || FilenameUtils.wildcardMatch(name, mask);
	}

	public static boolean isPigAvailable() {
		try {
			Utils.class.getClassLoader().loadClass("org.apache.pig.Main");
		} catch (ClassNotFoundException e) {
			return false;
		} catch (NoClassDefFoundError e) {
			return false;
		}
		return true;
	}

	public static boolean isAmazonEMRPigAvailable() {
		try {
			FileSystem fs = FileSystem.get(AmazonEMRPigJarURI,
					new Configuration());
			FileStatus stat;

			stat = fs.getFileStatus(new Path(AmazonEMRPigJarURI.toString()));
			if (stat != null && !stat.isDir())
				return true;
		} catch (Exception e) {
			return false;
		}

		return false;
	}
	
	@SuppressWarnings("unchecked")
	public static File combineJars(File mainJar, File includeJarsLib) throws IOException{
		if(!mainJar.exists()) throw new IOException("Jar file " + mainJar + " does not exist");
		if(!includeJarsLib.exists() && !includeJarsLib.isDirectory()) throw new IOException("Folder " + includeJarsLib + " does not exist");
		File jarDir = File.createTempFile("pig", "-unpacked");
		jarDir.deleteOnExit();
		File jarFile = File.createTempFile("pig", ".jar");
		jarFile.deleteOnExit();
		//unpack
		if(jarDir.exists())FileUtils.deleteQuietly(jarDir);
		if(!jarDir.mkdirs()){
			throw new IOException("can not create folder " + jarDir.getAbsolutePath());
		}
		RunJar.unJar(mainJar, jarDir);
		File libdir = new File(jarDir.getAbsolutePath(), "lib");
		libdir.mkdir();
		Collection<File> includeJars = FileUtils.listFiles(includeJarsLib, FileFilterUtils.suffixFileFilter(".jar"), FileFilterUtils.trueFileFilter());
		for(File includeJar : includeJars){
			FileUtils.copyFileToDirectory(includeJar, libdir);
		}
		//pack
		JarArchiveOutputStream jarOutputStream = null;
		try{
			jarOutputStream = new JarArchiveOutputStream(
					new FileOutputStream(jarFile));
			for(File f : (Collection<File>)FileUtils.listFiles(jarDir, FileFilterUtils.trueFileFilter(), FileFilterUtils.trueFileFilter())){
				String entryName = f.getAbsolutePath().substring(jarDir.getAbsolutePath().length() + 1, f.getAbsolutePath().length());
				jarOutputStream.putArchiveEntry(new JarArchiveEntry(entryName));
				if (!f.isDirectory()) {
					InputStream in = null;
					try {
						byte[] buffer = new byte[8192];
						in = new FileInputStream(f);
						int i;
						while ((i = in.read(buffer)) != -1) {
							jarOutputStream.write(buffer, 0, i);
						}
						in.close();
						jarOutputStream.closeArchiveEntry();
					} finally {
						if (in != null)
							in.close();
					}
				}
			}
		}
		finally{
			if(jarOutputStream != null)jarOutputStream.close();
		}
		return jarFile;
	}
	
	@SuppressWarnings("unchecked")
	public static String getCloudEraPigJar() {
		try {
			File cloudEraPigDir = new File(CloudEraPigDirPath);
			if(cloudEraPigDir.exists() && cloudEraPigDir.isDirectory()){
				Collection<File> files = (Collection<File>)FileUtils.listFiles(cloudEraPigDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
				for(File f : files){
					if(f.getName().startsWith("pig") && f.getName().endsWith(".jar")){
						return f.getAbsolutePath();
					}
				}
			}
		} catch (Exception e) {
			return null;
		}

		return null;
	}

	public static String replaceVariables(Context context, String value) {
		Matcher matcher = VARIABLE_PATTERN.matcher(value);
		StringBuilder outputValue = new StringBuilder();
		int curPos = 0;
		while (matcher.find()) {
			int start = matcher.start();
			int end = matcher.end();
			String variable = value.substring(start + 2, end - 1);
			outputValue.append(value.substring(curPos, start));
			if (!StringUtils.isEmpty(context.getString(variable))) {
				outputValue.append(context.getString(variable));
			} else {
				outputValue.append("*");
			}
			curPos = end;
		}
		outputValue.append(value.substring(curPos, value.length()));
		return outputValue.toString();
	}

	public static Path resolvePath(String pathStr, String workFolder) {
		Path path = new Path(pathStr);
		if (!path.isAbsolute() && !StringUtils.isEmpty(workFolder))
			path = new Path(workFolder, path);
		return path;
	}

}