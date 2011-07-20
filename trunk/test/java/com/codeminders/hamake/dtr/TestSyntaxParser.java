package com.codeminders.hamake.dtr;

import java.io.*;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;
import org.xml.sax.SAXException;

import com.codeminders.hamake.*;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.params.HamakeParameter;
import com.codeminders.hamake.syntax.BaseSyntaxParser;
import com.codeminders.hamake.syntax.InvalidMakefileException;
import com.codeminders.hamake.task.*;

public class TestSyntaxParser {
	
	private File tempDir;

	@After
	public void tearDown() {
		FileUtils.deleteQuietly(tempDir);
	}

	@Before
	public void setUp() {
		tempDir = HelperUtils.generateTemporaryDirectory();
	}
	
	@Test
	public void testFull() throws IOException, ParserConfigurationException, SAXException, InvalidMakefileException, PigNotFoundException, InvalidContextStateException{
		File localHamakeFile = HelperUtils.getHamakeTestResource("test-full.xml");
		File depPath = new File(tempDir, "deppath");
		depPath.mkdirs();
		File someDir = new File(tempDir, "somedir");
		someDir.mkdirs();
		File referrersDir = new File(tempDir, "referrers");
		referrersDir.mkdirs();
		HelperUtils.generateTemporaryFiles(referrersDir.getAbsolutePath(), 3, ".log");
		HelperUtils.generateTemporaryFiles(referrersDir.getAbsolutePath(), 3, ".fog");
		File relatedOneDir = new File(tempDir, "related/one");
		relatedOneDir.mkdirs();
		File relatedTwoDir = new File(tempDir, "related/two");
		relatedTwoDir.mkdirs();
		File revDir = new File(tempDir, "rev");
		HelperUtils.generateTemporaryFiles(revDir.getAbsolutePath(), 2, ".file");
		File revOutDir = new File(tempDir, "revout");
		HelperUtils.generateTemporaryFiles(revOutDir.getAbsolutePath(), 4, ".file");
		String tempDirPath = tempDir.getAbsolutePath().toString();
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("tmpdir", tempDirPath);
		context.setForbidden(Context.FOREACH_VARS_PREFIX + "path", tempDir.getAbsolutePath().toString() + "/referrers/1.log");
		context.setForbidden(Context.FOREACH_VARS_PREFIX + "basename", "1");
		
		Hamake make = BaseSyntaxParser.parse(context, new FileInputStream(localHamakeFile));
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
		Assert.assertEquals(1, mdtr1.getTrashBucket().getPath(context).size());
		Assert.assertEquals(false, mdtr1.isCopyIncorrectFile());
		MapReduce mr = (MapReduce)mdtr1.getTask();
		Assert.assertEquals(new File(tempDir.getAbsoluteFile().toString() + "/datamining.jar"), new File(mr.getJar()));
		Assert.assertEquals("us.imageshack.datamining.Access2Referrers", mr.getMain());
		Assert.assertEquals(1, mr.getParameters().size());
		Assert.assertEquals(new File(tempDirPath + "/referrers/1.log," + tempDirPath + "/related/1/1.txt"),
                new File(mr.getParameters().get(0).get(context)));
		Assert.assertEquals("[somepath]", mr.getClasspath().get(0).getPath(context).toString());
		//2nd foreach
		Foreach mdtr2 = (Foreach)make.getTasks().get(1);
		Assert.assertEquals("foreach2", mdtr2.getName());
		Assert.assertEquals(6, mdtr2.getInputs().get(0).getPath(context).size());
		Assert.assertEquals(2, mdtr2.getOutputs().size());
		Assert.assertEquals(0, mdtr2.getDeps().size());
		Assert.assertEquals(1, mdtr2.getTrashBucket().getPath(context).size());
		Assert.assertEquals(true, mdtr2.isCopyIncorrectFile());
		Assert.assertTrue(mdtr2.getTask() instanceof Exec);
		Exec ex = (Exec)mdtr2.getTask();
		Assert.assertEquals("cat", ex.getBinary().toString());
		Assert.assertEquals(3, ex.getParameters().size());
		Assert.assertEquals(new File(tempDirPath + "/referrers/1.log"),
                new File(ex.getParameters().get(0).get(context)));
		Assert.assertEquals(">", ex.getParameters().get(1).get(context));
		Assert.assertEquals(
                new File(tempDirPath + "/related/image_domains1"),
                new File(ex.getParameters().get(2).get(context)));
		//reduce
		Fold fold = (Fold)make.getTasks().get(2);
		Assert.assertEquals("reduce", fold.getName());
		Assert.assertEquals(1, fold.getInputs().size());
		Assert.assertEquals(3, fold.getInputs().get(0).getPath(context).size());
		Assert.assertEquals(2, fold.getOutputs().size());
		Assert.assertEquals(
                new File(tempDirPath + "/somedir"),
                new File(fold.getOutputs().get(0).getPath(context).get(0).toString()));
		Assert.assertEquals(4, fold.getOutputs().get(1).getPath(context).size());
		Assert.assertEquals(0, fold.getDeps().size());
		Assert.assertTrue(fold.getTask() instanceof Pig);
		Pig pig = (Pig)fold.getTask();
		Assert.assertEquals(
                new File(tempDirPath + "/median.pig"),
                new File(pig.getScript().toString()));
		Assert.assertEquals(3, pig.getParameters().size());
		Assert.assertEquals(tempDirPath + FilenameUtils.normalize("/somedir/somefile"), FilenameUtils.normalize(pig.getParameters().get(0).get(context)));
		Assert.assertEquals("-jobconf jcname=jcvalue", pig.getParameters().get(1).get(context));
	}
	
	@Test
	public void testMinimal() throws IOException, InvalidContextStateException, ParserConfigurationException, SAXException, InvalidMakefileException, PigNotFoundException{
		File localHamakeFile = HelperUtils.getHamakeTestResource("test-minimal.xml");
		File fileset = new File(tempDir, "fileset");
		fileset.mkdirs();
		HelperUtils.generateTemporaryFiles(fileset.getAbsolutePath(), 3);
		
		File folder = new File(tempDir, "folder");
		folder.mkdirs();
		
		File file = new File(tempDir, "file");
		file.mkdirs();
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("tmpdir", tempDir.getAbsolutePath().toString());
		
		Hamake make = BaseSyntaxParser.parse(context, new FileInputStream(localHamakeFile));
		Assert.assertNotNull(make.getProjectName());
		Assert.assertTrue(StringUtils.isEmpty(make.getDefaultTarget()));
		//1st foreach
		Foreach mdtr1 = (Foreach)make.getTasks().get(0);
		Assert.assertNotNull(mdtr1.getName());
		Assert.assertEquals(3, mdtr1.getInputs().get(0).getPath(context).size());
		Assert.assertEquals(0, mdtr1.getInputs().get(0).getGeneration());
		Assert.assertEquals(1, mdtr1.getOutputs().get(0).getPath(context).size());
		Assert.assertEquals(0, mdtr1.getOutputs().get(0).getGeneration());
		Assert.assertEquals(Long.MAX_VALUE, mdtr1.getOutputs().get(0).getValidityPeriod());
		Assert.assertEquals(0, mdtr1.getDeps().size());
		Assert.assertEquals(null, mdtr1.getTrashBucket());
		Assert.assertEquals(false, mdtr1.isCopyIncorrectFile());
		Assert.assertTrue(mdtr1.getTask() instanceof MapReduce);
		MapReduce mr = (MapReduce)mdtr1.getTask();
		Assert.assertEquals(
                new File(tempDir.getAbsoluteFile().toString() + "/datamining.jar"),
                new File(mr.getJar()));
		Assert.assertEquals("us.imageshack.datamining.Access2Referrers", mr.getMain());
		Assert.assertEquals(1, mr.getParameters().size());
		Assert.assertEquals(">", mr.getParameters().get(0).get(context));
		Assert.assertNotNull(((HamakeParameter)mr.getParameters().get(0)).getName());
	}
}
