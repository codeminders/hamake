package com.codeminders.hamake;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.FileDataFunction;
import com.codeminders.hamake.params.*;

public class TestHamakeParameter {
	
	@Test
	public void testGet() throws IOException, InvalidContextStateException{
		//simple case
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("input", "/input/in");
		context.set("output", "/output/out");
		context.set("gesse.file1", new FileDataFunction("gesse_german_stepnoi_volk (1).fb2"));
		context.set("gesse.file2", new FileDataFunction("gesse_german_stepnoi_volk%20(1).fb2"));
		Literal input = new Literal("${input}");
		Literal arrowSymbol = new Literal(" > ");
		Literal output = new Literal("${output}");
		HamakeParameter params = new HamakeParameter(Arrays.asList(input, arrowSymbol, output), new AppendConcatFunction(), new IdentityProcessingFunction());
		Assert.assertEquals("/input/in > /output/out", params.get(context));
		Reference ref1 = new Reference("gesse.file1");
		Assert.assertEquals("gesse_german_stepnoi_volk (1).fb2", ref1.getValue(context, new AppendConcatFunction()));
	}
	
	@Test
	public void testNormalizePathProcessingFunction() throws InvalidContextStateException, IOException{
		Context context = new Context(new Configuration(), null, false, false, false);
		context.set("input", "c:/somefolder/somefile");
		Literal input = new Literal("${input}");
		Assert.assertEquals(FilenameUtils.normalize("c:\\somefolder\\somefile"), new NormalizePathProcessingFunction().process(input.getValue(context)));
		context.set("reference", new FileDataFunction("/somefolder/../somefolder/somefile"));
		Reference reference = new Reference("reference");
		Assert.assertEquals("/somefolder/somefile", new NormalizePathProcessingFunction().process(reference.getValue(context, new AppendConcatFunction())));
		context.set("input1", "somekey/../somekey/somevalue");
		Literal input1 = new Literal("${input1}");
		Assert.assertEquals("somekey/../somekey/somevalue", new NormalizePathProcessingFunction().process(input1.getValue(context)));
		context.set("input2", "\"c:/somefolder/somefile\"");
		Literal input2 = new Literal("${input2}");
		Assert.assertEquals(FilenameUtils.normalize("\"c:\\somefolder\\somefile\""), new NormalizePathProcessingFunction().process(input2.getValue(context)));
	}
	
}

