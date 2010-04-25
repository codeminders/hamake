package com.codeminders.hamake.dtr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.InvalidContextVariableException;
import com.codeminders.hamake.PigNotFoundException;
import com.codeminders.hamake.TestHelperUtils;
import com.codeminders.hamake.dtr.Fold;
import com.codeminders.hamake.dtr.Foreach;
import com.codeminders.hamake.syntax.BaseSyntaxParser;
import com.codeminders.hamake.syntax.InvalidMakefileException;
import com.codeminders.hamake.task.Exec;
import com.codeminders.hamake.task.MapReduce;
import com.codeminders.hamake.task.Pig;

public class TestSyntaxParser {
	
	private File tempDir;

	@After
	public void tearDown() {
		FileUtils.deleteQuietly(tempDir);
	}

	@Before
	public void setUp() {
		tempDir = TestHelperUtils.generateTemporaryDirectory();
	}
	
	@Test
	public void testCorrectHamakefile() throws FileNotFoundException, IOException, ParserConfigurationException, SAXException, InvalidMakefileException, PigNotFoundException, InvalidContextVariableException{
		File localHamakeFile = new File(TestHelperUtils.getHamakefilesDir() + File.separator + "hamakefile-testsyntax.xml");
		File depPath = new File(tempDir, "deppath");
		depPath.mkdirs();
		File someDir = new File(tempDir, "somedir");
		someDir.mkdirs();
		File referrersDir = new File(tempDir, "referrers");
		referrersDir.mkdirs();
		TestHelperUtils.generateTemporaryFiles(referrersDir.getAbsolutePath(), 3, ".log");
		TestHelperUtils.generateTemporaryFiles(referrersDir.getAbsolutePath(), 3, ".fog");
		File relatedOneDir = new File(tempDir, "related/one");
		relatedOneDir.mkdirs();
		File relatedTwoDir = new File(tempDir, "related/two");
		relatedTwoDir.mkdirs();
		File revDir = new File(tempDir, "rev");
		TestHelperUtils.generateTemporaryFiles(revDir.getAbsolutePath(), 2, ".file");
		File revOutDir = new File(tempDir, "revout");
		TestHelperUtils.generateTemporaryFiles(revOutDir.getAbsolutePath(), 4, ".file");
		String tempDirPath = tempDir.getAbsolutePath().toString();
		Context context = new Context();
		context.set("tmpdir", tempDirPath);
		context.setForeach("path", tempDir.getAbsolutePath().toString() + "/referrers/1.log");
		context.setForeach("basename", "1");
		
		Hamake make = BaseSyntaxParser.parse(context, new FileInputStream(localHamakeFile), true);
		//project
		Assert.assertEquals("test-syntax", make.getProjectName());
		Assert.assertEquals("foreach1", make.getDefaultTarget());
		Assert.assertEquals(3, make.getTasks().size());
		//1st foreach
		Foreach mdtr1 = (Foreach)make.getTasks().get(0);
		Assert.assertEquals("foreach1", mdtr1.getName());
		Assert.assertEquals(3, mdtr1.getInputs().get(0).getPath(context).size());
		Assert.assertEquals(1, mdtr1.getOutputs().size());
		Assert.assertEquals(2, mdtr1.getOutputs().get(0).getPath(context).size());
		Assert.assertEquals(2, mdtr1.getOutputs().get(0).getGeneration());
		Assert.assertEquals(60*60*24*2, mdtr1.getOutputs().get(0).getValidityPeriod());
		Assert.assertEquals(1, mdtr1.getDeps().size());
		Assert.assertTrue(mdtr1.getTask() instanceof MapReduce);
		MapReduce mr = (MapReduce)mdtr1.getTask();
		Assert.assertEquals(tempDir.getAbsoluteFile().toString() + "/datamining.jar", mr.getJar());
		Assert.assertEquals("us.imageshack.datamining.Access2Referrers", mr.getMain());
		Assert.assertEquals(1, mr.getParameters().size());
		Assert.assertEquals(tempDirPath + "/referrers/1.log," + tempDirPath + "/related/1/1.txt", mr.getParameters().get(0).get(context));
		//2nd foreach
		Foreach mdtr2 = (Foreach)make.getTasks().get(1);
		Assert.assertEquals("foreach2", mdtr2.getName());
		Assert.assertEquals(6, mdtr2.getInputs().get(0).getPath(context).size());
		Assert.assertEquals(2, mdtr2.getOutputs().size());
		Assert.assertEquals(0, mdtr2.getDeps().size());
		Assert.assertTrue(mdtr2.getTask() instanceof Exec);
		Exec ex = (Exec)mdtr2.getTask();
		Assert.assertEquals("cat", ex.getBinary().toString());
		Assert.assertEquals(3, ex.getParameters().size());
		Assert.assertEquals(tempDirPath + "/referrers/1.log", ex.getParameters().get(0).get(context));
		Assert.assertEquals(">", ex.getParameters().get(1).get(context));
		Assert.assertEquals(tempDirPath + "/related/image_domains1", ex.getParameters().get(2).get(context));
		//reduce
		Fold fold = (Fold)make.getTasks().get(2);
		Assert.assertEquals("reduce", fold.getName());
		Assert.assertEquals(1, fold.getInputs().size());
		Assert.assertEquals(3, fold.getInputs().get(0).getPath(context).size());
		Assert.assertEquals(2, fold.getOutputs().size());
		Assert.assertEquals(tempDirPath + "/somedir", fold.getOutputs().get(0).getPath(context).get(0).toString());
		Assert.assertEquals(4, fold.getOutputs().get(1).getPath(context).size());
		Assert.assertEquals(0, fold.getDeps().size());
		Assert.assertTrue(fold.getTask() instanceof Pig);
		Pig pig = (Pig)fold.getTask();
		Assert.assertEquals(tempDirPath + "/median.pig", pig.getScript().toString());
		Assert.assertEquals(3, pig.getParameters().size());
		Assert.assertEquals("*", pig.getParameters().get(0).get(context));
		Assert.assertEquals("-jobconf jcname=jcvalue", pig.getParameters().get(1).get(context));
	}
}
