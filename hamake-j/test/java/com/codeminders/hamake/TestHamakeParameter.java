package com.codeminders.hamake;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.codeminders.hamake.params.AppendConcatFunction;
import com.codeminders.hamake.params.HamakeParameter;
import com.codeminders.hamake.params.IdentityProcessingFunction;
import com.codeminders.hamake.params.Literal;

public class TestHamakeParameter {
	
	@Test
	public void testGet() throws IOException, InvalidContextVariableException{
		//simple case
		Context context = Context.initContext(new Configuration(), null, Hamake.HAMAKE_VERSION, false);
		context.set("input", "/input/in");
		context.set("output", "/output/out");
		Literal input = new Literal("${input}");
		Literal arrowSymbol = new Literal(" > ");
		Literal output = new Literal("${output}");
		HamakeParameter params = new HamakeParameter(Arrays.asList(input, arrowSymbol, output), new AppendConcatFunction(), new IdentityProcessingFunction());
		Assert.assertEquals("/input/in > /output/out", params.get(context));
	}
	
}

