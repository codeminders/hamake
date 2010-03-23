package com.codeminders.hamake;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.codeminders.hamake.commands.HadoopCommand;
import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;

public class TestMakefileParser {
	
	@Test
	public void testSyntax() throws FileNotFoundException, IOException, ParserConfigurationException, SAXException, InvalidMakefileException{
		MakefileParser parser = new MakefileParser();
		Hamake make = new Hamake();
		File localHamakeFile = new File("hamakefile-testsyntax.xml");
		make = parser.parse(new FileInputStream(localHamakeFile), null, true);
		MapTask map1 = (MapTask)make.getTasks().get(0);
		MapTask map2 = (MapTask)make.getTasks().get(1);
		ReduceTask reduce = (ReduceTask)make.getTasks().get(2);
		//Assert project element is correct
		Assert.assertEquals("test-syntax", make.getProjectName());
		Assert.assertEquals("map1", make.defaultTarget);
		//Assert project project/config/property is correct
		Assert.assertEquals("/root/related/domain_referrers_totals", map1.getOutputs().get(0).getLoc());
		Assert.assertEquals("/base/datamining.jar", ((HadoopCommand)map1.getCommand()).getJar());
		//Assert project/{reduce|map}/taskdep is correct
		Assert.assertTrue(reduce.getTaskDeps().contains("map1"));
		Assert.assertTrue(reduce.getTaskDeps().contains("map2"));
		//
	}
}
