package com.codeminders.hamake.perfomance;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StressTestHamake {
	
	public static final Log LOG = LogFactory.getLog(StressTestHamake.class);
	
	public static void main(String[] args) throws IOException{
		float loadFactor = 0.5F;
		if(args.length == 1){
			int i = Integer.parseInt(args[0]) % 100;
			if(i > 0){
				loadFactor = (float)i / 100;
			}
		}
		else if(args.length > 1){
			LOG.error("Usage: StressTestHamake [load factor in %]");
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path tempDir = new Path(new Path(conf.get("hadoop.tmp.dir")), "hamake-stress-test");
		if(fs.exists(tempDir)){
			fs.delete(tempDir, true);
		}
		fs.mkdirs(tempDir);
		try {
			Class[] stressTestClasses = getClasses(StressTestHamake.class.getPackage().getName());
			for(Class stressTestClass : stressTestClasses){
				if(!stressTestClass.getName().equals(StressTestHamake.class.getName()) && !stressTestClass.getName().equals(StressTest.class.getName())){
					if(!stressTestClass.isInterface() && !stressTestClass.isAnonymousClass() && !stressTestClass.isEnum() && !stressTestClass.isMemberClass()){						
						try{
							Object obj = stressTestClass.newInstance();
							if(obj instanceof StressTest){
								StressTest stressTest = (StressTest)obj;
								Path testTempDir = new Path(tempDir, UUID.randomUUID().toString());
								fs.mkdirs(testTempDir);
								fs.deleteOnExit(testTempDir);
								stressTest.setConf(conf);
								stressTest.setTempDir(tempDir);
								stressTest.setLoadFactor(loadFactor);
								LOG.info("Launching stress test " + stressTestClass.getName() + ", temporary directory is " + testTempDir.toString());
								Thread th = new Thread(stressTest);
								th.start();
								th.join();
								if(stressTest.getException() != null){
									throw new Exception(stressTest.getException());
								}
							}
						}
						catch(InstantiationException e){
							LOG.error("Can not instantiate class " + stressTestClass.getName());
						}
					}
				}
			}
			LOG.info("All stress tests are done");
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			fs.delete(tempDir, true);
		} 
	}
	
	private static Class[] getClasses(String packageName)
            throws ClassNotFoundException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        String path = packageName.replace('.', File.separatorChar);
        Enumeration<URL> resources = classLoader.getResources(path);
        List<File> dirs = new ArrayList<File>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }
        ArrayList<Class> classes = new ArrayList<Class>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes.toArray(new Class[classes.size()]);
    }
	
	private static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class> classes = new ArrayList<Class>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }
	
}
