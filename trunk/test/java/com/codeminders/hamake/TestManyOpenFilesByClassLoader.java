package com.codeminders.hamake;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.ArrayList;

public class TestManyOpenFilesByClassLoader {
    public static void main(String[] args) throws Exception
    {
    	String javaHome = System.getProperty("java.home");
    	File f = new File(javaHome, "../lib/jconsole.jar");
        ArrayList<URLConnection> list = new ArrayList<URLConnection>();
        byte[] buf = new byte[(int)f.length()];

        FileInputStream in = null;
        boolean unload = args.length > 0 && "unload".equalsIgnoreCase(args[args.length-1]);

        try
        {
            in = new FileInputStream(f);
        	in.read(buf);
        } finally {
        	if (in != null) in.close();
        }

        try
        {
        	for (int j=0;j<20;j++)
        	{
	            for (int i=0;i<100;i++)
	            {

	                File tmpFile = File.createTempFile(f.getName(), ".jar");

	                FileOutputStream out = null;
	                try
	                {
		                out = new FileOutputStream(tmpFile);
	                	out.write(buf);
	                } finally {
	                	if (out != null) out.close();
	                }

	                tmpFile.deleteOnExit();

	                URL jarUrl = new URL("jar", "", -1, tmpFile.toURI().toString() + "!/");

	                URLConnection c = jarUrl.openConnection();
	                c.setUseCaches(true);

	                ClassLoader cLoader = new URLClassLoader(new URL[] {jarUrl}, null);

	                cLoader.loadClass("sun.tools.jconsole.JConsole");

	                list.add(c);
	            }

	            if (unload)
		        	for (URLConnection c:list)
		        		((JarURLConnection)c).getJarFile().close();

	        	list.clear();

        	}
        }
        catch (Throwable e)
        {
            e.printStackTrace();
        }

        return ;
    }
}

