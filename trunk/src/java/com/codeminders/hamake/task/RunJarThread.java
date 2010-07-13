package com.codeminders.hamake.task;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;

import com.codeminders.hamake.Utils;

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
    protected File workDir, file;
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
        try {
            String usage = "RunJar jarFile [mainClass] args...";

            if (args.length < 1) {
                System.err.println(usage);
                System.exit(-1);
            }

            String fileName = args[firstArg++];
            file = new File(fileName);

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
                    System.err.println(usage);
                    System.exit(-1);
                }
                mainClassName = args[firstArg++];
            }
            mainClassName = mainClassName.replaceAll("/", ".");

            File tmpDir = new File(new Configuration().get("hadoop.tmp.dir"));
            tmpDir.mkdirs();
            if (!tmpDir.isDirectory()) {
                System.err.println("Mkdirs failed to create " + tmpDir);
                System.exit(-1);
            }
            workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
            workDir.delete();
            workDir.mkdirs();
            if (!workDir.isDirectory()) {
                System.err.println("Mkdirs failed to create " + workDir);
                System.exit(-1);
            }

            unJar(file, workDir);

            tt[0] = null;
            start();
            join();
        }
        finally {
            try {
                FileUtil.fullyDelete(workDir);
            } catch (IOException e) {
            }
        }

        if (tt[0] != null)
            throw tt[0];
    }

    @Override
    public void run() {
        try {
            ArrayList<URL> classPath = new ArrayList<URL>();
            classPath.add(new File(workDir + "/").toURL());
            final ArrayList<JarURLConnection> jarConnections = new ArrayList<JarURLConnection>();
            {
        		final URL jarURL = new URL(
                        "jar", "", -1, (new StringBuilder()).append(file.toURL()).append("!/").toString()
                );
                // cache all the connections to JAR files
                JarURLConnection jarConnection = (JarURLConnection) jarURL.openConnection();
                jarConnection.setUseCaches(true);
                jarConnection.getJarFile();
                jarConnections.add(jarConnection);
                classPath.add(jarURL);
        	}
            classPath.add(new File(workDir, "classes/").toURL());
            File[] libs = new File(workDir, "lib").listFiles();
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

                    setDefaultConfiguration(classPath);
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
                }
                catch (InvocationTargetException e) {
                    throw e.getTargetException();
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

    public boolean setDefaultConfiguration(ArrayList<URL> classPath)
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
            }
        } catch (Exception e) {
            LOG.error("Failed to set default configuration of Hadoop job");
            return false;
        }

        return true;
    }
}

