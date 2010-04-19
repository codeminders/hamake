package com.codeminders.hamake;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.codeminders.hamake.dtr.Fold;
import com.codeminders.hamake.dtr.Foreach;
import com.codeminders.hamake.params.HamakeParameter;
import com.codeminders.hamake.params.JobConfParam;
import com.codeminders.hamake.syntax.BaseSyntaxParser;
import com.codeminders.hamake.syntax.InvalidMakefileException;
import com.codeminders.hamake.task.Exec;
import com.codeminders.hamake.task.MapReduce;
import com.codeminders.hamake.task.Pig;

public class TestSyntaxParser {
	
	@Test
	public void testProject() throws FileNotFoundException, IOException, ParserConfigurationException, SAXException, InvalidMakefileException, PigNotFoundException{
		File localHamakeFile = new File(TestHelperUtils.getHamakefilesDir() + File.separator + "hamakefile-testsyntax.xml");
		Context context = new Context();
		Hamake make = BaseSyntaxParser.parse(new Context(), new FileInputStream(localHamakeFile), null, true);
		//project
		Assert.assertEquals("test-syntax", make.getProjectName());
		Assert.assertEquals("foreach1", make.defaultTarget);
//		Assert.assertEquals(3, make.getTasks().size());
//		//1st foreach
//		Foreach mdtr = (Foreach)make.getTasks().get(0);
//		Assert.assertEquals("foreach1", mdtr.getName());
//		Assert.assertEquals("/root/referrers", mdtr.getXinput().getPathName().toString());
//		Assert.assertEquals("*.log", mdtr.getXinput().getMask());
//		Assert.assertEquals("foreach1_id", mdtr.getXinput().getID());
//		Assert.assertEquals("/base/somefile.txt", mdtr.getDeps().get(0).getPathName().toString());
//		Assert.assertEquals("/root/related/domain_referrers_totals", ((HamakePath)mdtr.getOutputs().get(0)).getPathName().toString());
//		Assert.assertEquals(1, ((HamakePath)mdtr.getOutputs().get(0)).getGen());
//		MapReduce hadoopCommand = (MapReduce)mdtr.getCommand();
//		Assert.assertEquals("/base/datamining.jar", hadoopCommand.getJar());
//		Assert.assertEquals("us.imageshack.datamining.Access2Referrers", hadoopCommand.getMain());
//		List<Param> parameters = hadoopCommand.getParameters();
//		Assert.assertEquals("${input}", ((HamakeParameter)parameters.get(0)).getValue());
//		Assert.assertEquals("${output}", ((HamakeParameter)parameters.get(1)).getValue());		
//		//2nd foreach
//		mdtr = (Foreach)make.getTasks().get(1);
//		Assert.assertEquals("foreach2", mdtr.getName());
//		Assert.assertEquals("/root/related/domain_referrers_totals", mdtr.getXinput().getPathName().toString());
//		Assert.assertEquals(2, mdtr.getXinput().getGen());
//		Assert.assertEquals("/root/related/image_domains1", ((HamakePath)mdtr.getOutputs().get(0)).getPathName().toString());
//		Assert.assertEquals("/root/related/image_domains2", ((HamakePath)mdtr.getOutputs().get(1)).getPathName().toString());
//		Assert.assertEquals(2 * 60 * 60 * 24, ((HamakePath)mdtr.getOutputs().get(0)).getValidityPeriod());
//		Assert.assertEquals(2 * 60 * 60 * 24, ((HamakePath)mdtr.getOutputs().get(1)).getValidityPeriod());
//		Exec execCommand = (Exec)mdtr.getCommand();
//		Assert.assertEquals("cat", execCommand.getBinary().getPathName().toString());
//		parameters = execCommand.getParameters();
//		Assert.assertEquals("${input}", ((HamakeParameter)parameters.get(0)).getValue());
//		Assert.assertEquals(">", ((HamakeParameter)parameters.get(1)).getValue());
//		Assert.assertEquals("${output}", ((HamakeParameter)parameters.get(2)).getValue());
//		//reduce
//		Fold rdtr = (Fold)make.getTasks().get(2);	
//		Assert.assertEquals("reduce", rdtr.getName());
//		Assert.assertEquals("/base/somefile.txt", rdtr.getDeps().get(0).getPathName().toString());
//		Assert.assertEquals("/root/image_domains", rdtr.getInputs().get(0).getPathName().toString());
//		Assert.assertEquals(HamakePath.Variant.LIST, rdtr.getInputs().get(0).getVariant());
//		Assert.assertEquals(1, rdtr.getInputs().get(0).getGen());
//		Assert.assertEquals("/root/rev", rdtr.getInputs().get(1).getPathName().toString());
//		Assert.assertEquals("*.file", rdtr.getInputs().get(1).getMask());
//		Assert.assertEquals("/root/related/porn_stats.tmp", ((HamakePath)rdtr.getOutputs().get(0)).getPathName().toString());
//		Assert.assertEquals(15 * 60 * 60, ((HamakePath)rdtr.getOutputs().get(0)).getValidityPeriod());
//		Assert.assertEquals("/root/related", ((HamakePath)rdtr.getOutputs().get(1)).getPathName().toString());
//		Assert.assertEquals(15 * 60 * 60, ((HamakePath)rdtr.getOutputs().get(1)).getValidityPeriod());
//		Assert.assertEquals("*.file", ((HamakePath)rdtr.getOutputs().get(1)).getMask());
//		Assert.assertEquals(HamakePath.Variant.MASK, ((HamakePath)rdtr.getOutputs().get(1)).getVariant());
//		Pig pigCommand = (Pig)rdtr.getCommand();
//		Assert.assertEquals("median.pig", pigCommand.getScript().getPathName().toString());
//		parameters = pigCommand.getParameters();
//		Assert.assertEquals("${input}", ((HamakeParameter)parameters.get(0)).getValue());
//		Assert.assertEquals("jcname", ((JobConfParam)parameters.get(1)).getName());
//		Assert.assertEquals("jcvalue", ((JobConfParam)parameters.get(1)).getValue());
//		Assert.assertEquals("${output}", ((HamakeParameter)parameters.get(2)).getValue());
	}
}
