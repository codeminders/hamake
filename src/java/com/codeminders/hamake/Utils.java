package com.codeminders.hamake;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.jar.JarArchiveEntry;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.conf.Configuration;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.DataFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

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

	public static int execute(Context context, final String command) {
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

                final Process process = Runtime.getRuntime().exec(cmd);

                IOUtils.closeStream(process.getOutputStream());

                final Thread stdoutReader = new Thread()
                    {
                        @Override
                        public void run()
                        {
                            final InputStream inputStream = process.getInputStream();
                            try
                            {
                                IOUtils.copyBytes(inputStream, System.out, 8192, false);
                            }
                            catch (final IOException ex)
                            {
                                LOG.error(command + ": I/O error reading standard output", ex);
                            }
                            finally
                            {
                                IOUtils.closeStream(inputStream);
                            }
                        }
                    };
                stdoutReader.start();

                final Thread stderrReader = new Thread()
                    {
                        @Override
                        public void run()
                        {
                            final InputStream errorStream = process.getErrorStream();
                            try
                            {
                                IOUtils.copyBytes(errorStream, System.err, 8192, false);
                            }
                            catch (final IOException ex)
                            {
                                LOG.error(command + ": I/O error reading standard error", ex);
                            }
                            finally
                            {
                                IOUtils.closeStream(errorStream);
                            }
                        }
                    };
                stderrReader.start();

                final int exitCode = process.waitFor();

                stdoutReader.join();
                stderrReader.join();

                return exitCode;
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
			LOG
					.info("Downloading " + path + " to "
							+ dstFile.getAbsolutePath());
			fs.copyToLocalFile(srcPath, new Path(dstFile.getAbsolutePath()));
		} else
			throw new IOException("Path not found: " + path);
//		dstFile.deleteOnExit();
		return dstFile;

	}

	public static File removeManifestAttributes(File jarFile,
			List<String> attributesToRemove) throws IOException {
		JarFile jar = new JarFile(jarFile);
		JarArchiveOutputStream jarOutputStream = null;
		try {
			Attributes attributes = jar.getManifest().getMainAttributes();
			for (String attributeToRemove : attributesToRemove) {
				if (!StringUtils
						.isEmpty(attributes.getValue(attributeToRemove))) {
					File outputFile = File.createTempFile("hamake-", ".jar",
							jarFile.getParentFile());
					jarOutputStream = new JarArchiveOutputStream(
							new FileOutputStream(outputFile));
					Enumeration<JarEntry> entries = jar.entries();
					while (entries.hasMoreElements()) {
						JarEntry entry = (JarEntry) entries.nextElement();
						if (entry.getName().equalsIgnoreCase(
								JarFile.MANIFEST_NAME)) {
							File manifestParent = new File(jarFile.getParentFile() + File.separator + jarFile.getName() + "-unpacked");
							File tempManifest = new File(manifestParent, JarFile.MANIFEST_NAME);
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
							FileUtils.deleteDirectory(manifestParent);
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
					FileUtils.deleteQuietly(jarFile);
					jarFile = outputFile;
//					jarFile.deleteOnExit();
				}
			}
		} finally {
			if (jar != null)
				jar.close();
			if (jarOutputStream != null)
				jarOutputStream.close();
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
	public static File combineJars(File mainJar, File includeJarsLib)
			throws IOException {
		if (!mainJar.exists())
			throw new IOException("Jar file " + mainJar + " does not exist");
		if (!includeJarsLib.exists() && !includeJarsLib.isDirectory())
			throw new IOException("Folder " + includeJarsLib
					+ " does not exist");
		File jarFile = File.createTempFile(FilenameUtils.getBaseName(mainJar
				.getName()), ".jar");
		jarFile.deleteOnExit();
		File jarDir = File.createTempFile(mainJar.getName(), "-unpacked");
		if (jarDir.exists())
			FileUtils.deleteQuietly(jarDir);
		if (!jarDir.mkdirs()) {
			throw new IOException("can not create folder "
					+ jarDir.getAbsolutePath());
		}
		// unpack
		RunJar.unJar(mainJar, jarDir);
		File libdir = new File(jarDir.getAbsolutePath(), "lib");
		libdir.mkdir();
		Collection<File> includeJars = FileUtils.listFiles(includeJarsLib,
				FileFilterUtils.suffixFileFilter(".jar"), FileFilterUtils
						.trueFileFilter());
		for (File includeJar : includeJars) {
			FileUtils.copyFileToDirectory(includeJar, libdir);
		}
		// pack
		JarArchiveOutputStream jarOutputStream = null;
		try {
			jarOutputStream = new JarArchiveOutputStream(new FileOutputStream(
					jarFile));
			for (File f : (Collection<File>) FileUtils.listFiles(jarDir,
					FileFilterUtils.trueFileFilter(), FileFilterUtils
							.trueFileFilter())) {
				String entryName = f.getCanonicalPath().substring(
						jarDir.getCanonicalPath().length() + 1,
						f.getCanonicalPath().length()).replace("\\", "/");
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
		} finally {
			if (jarOutputStream != null)
				jarOutputStream.close();
			FileUtils.deleteDirectory(jarDir);
		}
		return jarFile;
	}

	@SuppressWarnings("unchecked")
	public static String getCloudEraPigJar() {
		try {
			File cloudEraPigDir = new File(CloudEraPigDirPath);
			if (cloudEraPigDir.exists() && cloudEraPigDir.isDirectory()) {
				Collection<File> files = (Collection<File>) FileUtils
						.listFiles(cloudEraPigDir, TrueFileFilter.INSTANCE,
								TrueFileFilter.INSTANCE);
				for (File f : files) {
					if (f.getName().startsWith("pig")
							&& f.getName().endsWith(".jar")) {
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
		String[] values = replaceVariablesMultiValued(context, value);
		if (values.length != 1) {
			throw new RuntimeException("not expecting multi-valued variables");
		}
		return values[0];
	}

	public static String[] replaceVariablesMultiValued(Context context, String value) {
		Matcher matcher = VARIABLE_PATTERN.matcher(value);
		String[] outputValues = null;
		StringBuilder outputValue = new StringBuilder();
		int outputValueCount = 1;
		boolean outputValueCountFixed = false;
		for (int outputValueIndex = 0; outputValueIndex < outputValueCount; ++outputValueIndex) {
			int curPos = 0;
			while (matcher.find()) {
				int start = matcher.start();
				int end = matcher.end();
				String variable = value.substring(start + 2, end - 1);
				outputValue.append(value.substring(curPos, start));

				Object var = context.get(variable);
				if (var instanceof String && !StringUtils.isEmpty((String) var)) {
					outputValue.append(var);
				} else if (var instanceof DataFunction) {
					DataFunction df = (DataFunction) var;
					outputValue.append(StringUtils.join(df.toString(context), " "));
				} else if (var instanceof String[]) {
					String[] a = (String[]) var;
					if (!outputValueCountFixed) {
						outputValueCount = a.length;
						if (outputValueCount == 0) {
							throw new RuntimeException("zero-length multi-valued variable");
						}
						outputValueCountFixed = true;
					} else if (a.length != outputValueCount) {
						throw new RuntimeException("multi-valued variables with mismatched lengths");
					}
					outputValue.append(a[outputValueIndex]);
				} else {
					if (context.getBoolean(Context.HAMAKE_PROPERTY_VERBOSE))
						LOG.warn("Variable or property "
								+ context.getString(variable) + " is not found.");
				}
				curPos = end;
			}
			outputValue.append(value.substring(curPos, value.length()));
			if (outputValues == null) {
				outputValues = new String[outputValueCount];
			}
			outputValues[outputValueIndex] = outputValue.toString();
			outputValue.setLength(0);
			matcher.reset();
		}
		return outputValues;
	}

	public static Path resolvePath(String pathStr, String workFolder) {
		Path path = new Path(pathStr);
		if (!path.isAbsolute() && !StringUtils.isEmpty(workFolder))
			path = new Path(workFolder, path);
		return path;
	}

	public static int[] getHadoopVersion() {
		int[] result = { 0, 0 };
		String[] version = VersionInfo.getVersion()
				.split("[.]|[+]|[-]|[_]|[,]");
		try {
			if (version.length >= 1) {
				result[0] = Integer.parseInt(version[0]);
			}
		} catch (NumberFormatException e) {
			LOG.error("Could not get Hadoop major version");
		}
		try {
			if (version.length >= 2) {
				result[1] = Integer.parseInt(version[1]);
			}
		} catch (NumberFormatException e) {
			LOG.error("Could not get Hadoop minor version");
		}
		return result;
	}

	public static String removeSchema(String path) throws URISyntaxException {
		if(!path.matches("^[\"]?([A-Z]|[a-z]){1}[\\:]{1}.*")){
			String schema = new URI(path).getScheme();
			if (!StringUtils.isEmpty(schema)) {
				return path.substring(schema.length() + 3);
			}
		}
		return path;
	}

	public static boolean compareFs(FileSystem srcFs, FileSystem destFs) {
		URI srcUri = srcFs.getUri();
		URI dstUri = destFs.getUri();
		if (srcUri.getScheme() == null) {
			return false;
		}
		if (!srcUri.getScheme().equals(dstUri.getScheme())) {
			return false;
		}
		String srcHost = srcUri.getHost();
		String dstHost = dstUri.getHost();
		if ((srcHost != null) && (dstHost != null)) {
			try {
				srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
				dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
			} catch (UnknownHostException ue) {
				return false;
			}
			if (!srcHost.equals(dstHost)) {
				return false;
			}
		} else if (srcHost == null && dstHost != null) {
			return false;
		} else if (srcHost != null && dstHost == null) {
			return false;
		}
		// check for ports
		if (srcUri.getPort() != dstUri.getPort()) {
			return false;
		}
		return true;
	}
	
	public static long recursiveGetModificationTime(FileSystem fs, Path p) throws IOException{
		FileStatus stat = fs.getFileStatus(p);
		long modificationTime = stat.getModificationTime();
		if(modificationTime == 0 && stat.isDir()){
			modificationTime = getDirMaxModificationTime(fs, modificationTime, stat);
		}
		return modificationTime;
	}
	
	private static long getDirMaxModificationTime(FileSystem fs, long modificationTime, FileStatus stat) throws IOException{
		FileStatus[] stats = fs.listStatus(stat.getPath());
		if(stats.length > 0){
			for(FileStatus s : stats){
				if(s.isDir()){
					long m = getDirMaxModificationTime(fs, modificationTime, s);
					if(m > modificationTime) modificationTime = m;
				}
				else if(s.getModificationTime() > modificationTime){
					modificationTime = s.getModificationTime();
				}
			}
		}
		return modificationTime;
	}
}
