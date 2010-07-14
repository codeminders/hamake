package com.codeminders.hamake.perfomance;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;



import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StressTestHamake {
	
	public static final Log LOG = LogFactory.getLog(StressTestHamake.class);
	
	public static void main(String[] args) throws IOException{
		Options options = new Options();
        options.addOption("l", "loadFactor", true, "set load factor of stress tests");
        CommandLineParser parser = new PosixParser();

        CommandLine line = null;
        try {
            line = parser.parse(options, args, false);
        }
        catch (ParseException ex) {
            new HelpFormatter().printHelp("hamake stress test", options);
            return;
        }
        float loadFactor = 0.5F;
        if (line.hasOption('l')){
        	int i = Integer.parseInt(line.getOptionValue('l').trim()) % 101;
			if(i > 0){
				loadFactor = (float)i / 100;
			}
        }
        Map<String, Boolean> runOnlyStressTests = new HashMap<String, Boolean>();
        List<Class> stressTestsToRun = new ArrayList<Class>();
        if(!line.getArgList().isEmpty()){
        	for(String tests : line.getArgs()){
        		if(!StringUtils.isEmpty(tests)){
	        		for(String test : tests.trim().split("[,]|[\\s]")){
	        			runOnlyStressTests.put(StressTestHamake.class.getPackage().getName() + "." + test, new Boolean(true));
	        		}
        		}
        	}
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
				boolean runTest = false;
				if(!stressTestClass.getName().equals(StressTestHamake.class.getName()) && !stressTestClass.getName().equals(StressTest.class.getName())){
					if(!stressTestClass.isInterface() && !stressTestClass.isAnonymousClass() && !stressTestClass.isEnum() && !stressTestClass.isMemberClass()){						
						if(runOnlyStressTests.isEmpty() || runOnlyStressTests.containsKey(stressTestClass.getName())){
							stressTestsToRun.add(stressTestClass);
						}
					}
				}
			}
			LOG.info("Will run " + stressTestsToRun.size() + " stress tests");
			for(Class stressTestClass : stressTestsToRun){
				try{
					Object obj = stressTestClass.newInstance();
					if(obj instanceof StressTest){
						StressTest stressTest = (StressTest)obj;
						Path testTempDir = new Path(tempDir, UUID.randomUUID().toString());
						fs.mkdirs(testTempDir);
						fs.deleteOnExit(testTempDir);
						stressTest.setConf(conf);
						stressTest.setTempDir(testTempDir);
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
			LOG.info("All stress tests are done");
		} catch (Exception e) {
			LOG.error("Exception when running stress tests", e);
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
