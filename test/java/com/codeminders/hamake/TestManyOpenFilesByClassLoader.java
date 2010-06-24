package com.codeminders.hamake;

import java.io.*;

import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.jar.JarOutputStream;

import org.junit.Test;

public class TestManyOpenFilesByClassLoader {
    private static File makeTestJar() throws IOException {
        // make JAR containing one file "fooboo.txt"
        File jarLibFile = File.createTempFile("classloader-test", ".jar");
        jarLibFile.deleteOnExit();

        JarOutputStream jstream =
          new JarOutputStream(new FileOutputStream(jarLibFile));

        jstream.putNextEntry(new ZipEntry("fooboo.txt"));
        jstream.closeEntry();
        jstream.close();

        return jarLibFile;
    }

    public static void main(String[] args) throws Exception
    {
    	File f = makeTestJar();
        ArrayList<URLConnection> list = new ArrayList<URLConnection>();
        byte[] buf = new byte[(int)f.length()];

        FileInputStream in = null;
        boolean unload = args.length > 0 && "unload".equalsIgnoreCase(args[args.length-1]);

        try {
            in = new FileInputStream(f);
        	in.read(buf);
        } finally {
        	if (in != null) in.close();
        }

        try {
        	for (int j=0;j<20;j++) {
	            for (int i=0;i<100;i++) {
                    // clone our test JAR
	                File tmpFile = File.createTempFile(f.getName(), ".jar");
	                FileOutputStream out = null;
	                try {
		                out = new FileOutputStream(tmpFile);
	                	out.write(buf);
	                } finally {
	                	if (out != null) out.close();
	                }
	                tmpFile.deleteOnExit();

                    // open cached connection to just created clone of test JAR
	                URL jarUrl = new URL("jar", "", -1, tmpFile.toURI().toString() + "!/");
	                URLConnection c = jarUrl.openConnection();
	                c.setUseCaches(true);

	                ClassLoader cLoader = new URLClassLoader(new URL[] {jarUrl}, null);
                    // make sure ClassLoader loads our test JAR
                    cLoader.getResource("fooboo.txt");

	                list.add(c);
	            }

	            if (unload)
		        	for (URLConnection c:list)
		        		((JarURLConnection)c).getJarFile().close();

	        	list.clear();
        	}
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testManyOpenFiles() throws Exception{
    	TestManyOpenFilesByClassLoader.main(new String[] {"unload"});
    }
}

