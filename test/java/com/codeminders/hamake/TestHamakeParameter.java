package com.codeminders.hamake;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.FileDataFunction;
import com.codeminders.hamake.params.AppendConcatFunction;
import com.codeminders.hamake.params.HamakeParameter;
import com.codeminders.hamake.params.IdentityProcessingFunction;
import com.codeminders.hamake.params.Literal;
import com.codeminders.hamake.params.Reference;

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
		Assert.assertEquals("\"gesse_german_stepnoi_volk (1).fb2\"", ref1.getValue(context, new AppendConcatFunction()));
	}
	
}

