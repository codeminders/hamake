package com.codeminders.hamake;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.utils.TestHelperUtils;

public class TestHamake {

	@Test
	public void testLocalCpHamakefile() throws IOException, ParserConfigurationException, SAXException, InvalidMakefileException{
		File tempInDir = TestHelperUtils.generateTemporaryDirectory(null);
		File tempMap1OutDir = TestHelperUtils.generateTemporaryDirectory(null);
		File tempMap2OutDir = TestHelperUtils.generateTemporaryDirectory(null);
		File[] files = TestHelperUtils.generateTemporaryFiles(tempInDir.getAbsolutePath(), 10);
		MakefileParser parser = new MakefileParser();		
		Hamake make = new Hamake();
		File localHamakeFile = new File("hamakefile-local-cp.xml");
		make = parser.parse(new FileInputStream(localHamakeFile), true);
		Collection<Task> tasks = make.getTasks();
		Iterator<Task> i = tasks.iterator();
		while(i.hasNext()){
			Task task = i.next();
			if(task instanceof MapTask){
				MapTask m = (MapTask)task;
				if(task.getName().equals("map1")){
					m.setXinput(new Path(tempInDir.getAbsolutePath()));
					m.getOutputs().clear();
					m.getOutputs().add(new Path(tempMap1OutDir.getAbsolutePath()));
				}
				else if(task.getName().equals("map2")){
					m.setXinput(new Path(tempMap1OutDir.getAbsolutePath()));
					m.getOutputs().clear();
					m.getOutputs().add(new Path(tempMap2OutDir.getAbsolutePath()));
				}
			}
		}
		make.setFileSystem(FileSystem.get(new Configuration()));
		make.setNumJobs(2);
		make.run();
		Assert.assertEquals(10, FileUtils.listFiles(tempMap1OutDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
		Assert.assertEquals(10, FileUtils.listFiles(tempMap2OutDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).size());
	}
//	@Test
//	public void testSimpleHamakeFile() throws FileNotFoundException, IOException, ParserConfigurationException, SAXException, InvalidMakefileException{
//		MakefileParser parser = new MakefileParser();
//		Hamake make = new Hamake();
//		File classSizeHamakeFile = new File("test/resources/class-size.xml");
//		make = parser.parse(new FileInputStream(classSizeHamakeFile), true);
//		make.setFileSystem(FileSystem.get(new Configuration()));
//		make.setNumJobs(2);
//		make.run();
//		System.out.println("done");
//	}
}
