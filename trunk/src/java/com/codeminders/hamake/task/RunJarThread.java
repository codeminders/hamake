package com.codeminders.hamake.task;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;

import com.codeminders.hamake.Utils;
import com.codeminders.hamake.context.Context;

import java.io.*;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.util.jar.Manifest;
import java.util.*;
import java.net.*;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;

public class RunJarThread extends Thread {

    public static final Log LOG = LogFactory.getLog(RunJarThread.class);

    protected String[] args;
    protected Throwable[] tt = new Throwable[1];
    protected File unpackedJar, jar;
    protected String mainClassName = null;
    protected int firstArg = 0;
    protected File[] additionalJars;
    protected Configuration customHadoopConf;

    protected RunJarThread(File[] additionalJars, Configuration additionalConfiguration) {
        this.additionalJars = additionalJars;
        this.customHadoopConf = additionalConfiguration;
    }

    /**
     * Unpack a jar file into a directory.
     */
    public void unJar(File jarFile, File toDir) throws IOException {
        JarFile jar = new JarFile(jarFile);
        try {
            Enumeration entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = (JarEntry) entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = jar.getInputStream(entry);
                    try {
                        File file = new File(toDir, entry.getName());
                        if (!file.getParentFile().mkdirs()) {
                            if (!file.getParentFile().isDirectory()) {
                                throw new IOException("Mkdirs failed to create " +
                                        file.getParentFile().toString());
                            }
                        }
                        OutputStream out = new FileOutputStream(file);
                        try {
                            byte[] buffer = new byte[8192];
                            int i;
                            while ((i = in.read(buffer)) != -1) {
                                out.write(buffer, 0, i);
                            }
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            jar.close();
        }
    }

    public static void main(String[] args, File[] additionalJars, Configuration additionalConfiguration) throws Throwable {
        RunJarThread rj = new RunJarThread(additionalJars, additionalConfiguration);
        try {
            rj.start(args);
        }
        finally {
            rj = null;
            System.gc();
        }
    }

	public void start(String[] args) throws Throwable {
		this.args = args;
		String usage = "RunJar jarFile [mainClass] args...";

		if (args.length < 1) {
			throw new Exception("first argument of " + RunJarThread.class.getName() + ".start should be a jar file");
		}

		String fileName = args[firstArg++];
		jar = new File(fileName);

		JarFile jarFile;
		try {
			jarFile = new JarFile(fileName);
		} catch (IOException io) {
			throw new IOException("Error opening job jar: " + fileName);
		}

		Manifest manifest = jarFile.getManifest();
		if (manifest != null) {
			mainClassName = manifest.getMainAttributes().getValue("Main-Class");
		}
		jarFile.close();

		if (mainClassName == null) {
			if (args.length < 2) {
				throw new Exception("second argument of " + RunJarThread.class.getName() + ".start should be a main method");
			}
			mainClassName = args[firstArg++];
		}
		mainClassName = mainClassName.replaceAll("/", ".");

		unpackedJar = getCachedUnpackedJarFolder(jar);

		tt[0] = null;
		start();
		join();

		if (tt[0] != null)
			throw tt[0];
	}

    @Override
    public void run() {
        try {
            ArrayList<URL> classPath = new ArrayList<URL>();
            classPath.add(new File(unpackedJar + "/").toURL());
            final ArrayList<JarURLConnection> jarConnections = new ArrayList<JarURLConnection>();
            {
        		final URL jarURL = new URL(
                        "jar", "", -1, (new StringBuilder()).append(jar.toURL()).append("!/").toString()
                );
                // cache all the connections to JAR files
                JarURLConnection jarConnection = (JarURLConnection) jarURL.openConnection();
                jarConnection.setUseCaches(true);
                jarConnection.getJarFile();
                jarConnections.add(jarConnection);
                classPath.add(jarURL);
        	}
            classPath.add(new File(unpackedJar, "classes/").toURL());
            File[] libs = new File(unpackedJar, "lib").listFiles();
            URLClassLoader loader;

            try {
                if (libs != null) {
                    for (int i = 0; i < libs.length; i++) {
                        final URL jarURL = new URL(
                                "jar", "", -1, (new StringBuilder()).append(libs[i].toURL()).append("!/").toString()
                        );
                        // cache all the connections to JAR files
                        JarURLConnection jarConnection = (JarURLConnection) jarURL.openConnection();
                        jarConnection.setUseCaches(true);
                        jarConnection.getJarFile();
                        jarConnections.add(jarConnection);

                        classPath.add(jarURL);
                    }
                }
                File tempConfigurationFile = null;
                if (additionalJars.length > 0) {
                    for (int i = 0; i < additionalJars.length; i++) {
                        final URL jarURL = new URL(
                                "jar", "", -1, (new StringBuilder()).append(additionalJars[i].toURL()).append("!/").toString()
                        );

                        // cache all the connections to JAR files
                        JarURLConnection jarConnection = (JarURLConnection) jarURL.openConnection();
                        jarConnection.setUseCaches(true);
                        jarConnection.getJarFile();
                        jarConnections.add(jarConnection);

                        classPath.add(jarURL);
                    }

                    tempConfigurationFile = setDefaultConfiguration(classPath);
                }

                loader = new URLClassLoader(classPath.toArray(new URL[0]));

                Thread.currentThread().setContextClassLoader(loader);

                Class<?> mainClass = Class.forName(mainClassName, true, loader);
                Method main = mainClass.getMethod("main", new Class[]{
                        Array.newInstance(String.class, 0).getClass()
                });
                String[] newArgs = Arrays.asList(args)
                        .subList(firstArg, args.length).toArray(new String[0]);
                try {
                    main.invoke(null, new Object[]{newArgs});
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                } finally{
                	FileUtils.deleteQuietly(tempConfigurationFile);
                }
            }
            finally {
                // close opened JARs
                for (JarURLConnection c : jarConnections)
                    try {
                        c.getJarFile().close();
                        
                    }
                    catch (Throwable e) {
                    }
            }
            
        }
        catch (Throwable ex) {
            tt[0] = ex;
        }
    }

    public File setDefaultConfiguration(ArrayList<URL> classPath)
    {
        try
        {
            if (Utils.getHadoopVersion()[1] > 19) {
                File tempConfigurationFile = File.createTempFile("hamake-configuration-", ".xml");
                DataOutputStream writer = null;
                try {
                    writer = new DataOutputStream(new FileOutputStream(tempConfigurationFile));
                    Method writeXmlMethod = Configuration.class.getDeclaredMethod("writeXml", OutputStream.class);
                    writeXmlMethod.invoke(customHadoopConf, writer);
                }
                finally {
                    if (writer != null) writer.close();
                }

                classPath.add(tempConfigurationFile.getParentFile().toURI().toURL());
                Method addDefaultResourceMethod = Configuration.class.getDeclaredMethod("addDefaultResource", String.class);
                addDefaultResourceMethod.invoke(null, tempConfigurationFile.getName());
                return tempConfigurationFile;

            } else {
                List<String> tmpJars = new ArrayList<String>();
                String[] archives = customHadoopConf.get("mapred.cache.archives").split(",");
                for (String archive : archives) {
                    tmpJars.add(new Path(archive).toUri().getPath());
                }
                customHadoopConf.set("tmpjars", StringUtils.join(tmpJars, ","));
                Method setCommandLineConfigMethod = JobClient.class.getDeclaredMethod("setCommandLineConfig", Configuration.class);
                setCommandLineConfigMethod.setAccessible(true);
                setCommandLineConfigMethod.invoke(null, customHadoopConf);
                return null;
            }
        } catch (Exception e) {
            LOG.error("Failed to set default configuration of Hadoop job");
            return null;
        }
    }
    
    private File getCachedUnpackedJarFolder(File jar) throws IOException{
    	Cache cache = Context.cacheManager.getCache("MapReduceUnpackedJarCache");
    	if(cache.get(jar.hashCode()) == null){
            File unpackedJar = File.createTempFile(jar.getName(), "-unpacked");
            unpackedJar.delete();
            unpackedJar.mkdirs();
            if (!unpackedJar.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + unpackedJar);
            }
            unJar(jar, unpackedJar);
            cache.put(new Element(jar.hashCode(), unpackedJar));
    	}
    	return (File)cache.get(jar.hashCode()).getValue();
    }
}

