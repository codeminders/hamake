package com.codeminders.hamake.task;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.codeminders.hamake.*;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.syntax.BaseSyntaxParser;
import com.codeminders.hamake.syntax.InvalidMakefileException;

public class TestMapReduce {

	@Test
	public void testClasspath() throws InvalidContextStateException,
			IOException, ParserConfigurationException, SAXException,
			InvalidMakefileException, PigNotFoundException {
		Context context = new Context(new Configuration(), null, false, false,
				false);
		context.set("examples.jar", new File("testMapReduce.jar").getAbsoluteFile().toString());
		context.set("test.classpath", new File("testMapReduceLib").getAbsoluteFile().toString());
		File localHamakeFile = TestHamake.getHamakefile("hamakefile-testclasspath.xml");
		final Hamake make = BaseSyntaxParser.parse(context,
				new FileInputStream(localHamakeFile));
		make.setNumJobs(1);
		Assert.assertEquals(Hamake.ExitCode.OK, make.run());
	}

}
