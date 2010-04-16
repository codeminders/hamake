package com.codeminders.hamake;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import com.codeminders.hamake.params.HamakeParameter;
import com.codeminders.hamake.params.PathParam;

public class TestHamakeParameter {
	
	@Test
	public void testGet() throws IOException{
		//simple case
		Map<String, List<HamakePath>> dict = new HashMap<String, List<HamakePath>>();
		dict.put(PathParam.Type.inputfile.toString(), Arrays.asList(new HamakePath("/input/in")));
		dict.put(PathParam.Type.outputfile.toString(), Arrays.asList(new HamakePath("/output/out")));
		HamakeParameter input = new HamakeParameter("${input}");
		List<String> in = input.get(dict, FileSystem.get(new Configuration()));
		Assert.assertEquals("[/input/in]", in.toString());
		HamakeParameter output = new HamakeParameter("${output}");
		List<String> out = output.get(dict, FileSystem.get(new Configuration()));
		Assert.assertEquals("[/output/out]", out.toString());
		//double output
		dict.clear();
		dict.put(PathParam.Type.outputfile.toString(), Arrays.asList(new HamakePath("/output/out1"), new HamakePath("/output/out2")));
		out = output.get(dict, FileSystem.get(new Configuration()));
		Assert.assertEquals("[/output/out1, /output/out2]", out.toString());
		//input and output in a single line
		output = new HamakeParameter("${input} > ${output}");
		dict.clear();
		dict.put(PathParam.Type.inputfile.toString(), Arrays.asList(new HamakePath("/input/in")));
		dict.put(PathParam.Type.outputfile.toString(), Arrays.asList(new HamakePath("/output/out")));
		out = output.get(dict, FileSystem.get(new Configuration()));
		Assert.assertEquals("[/input/in, >, /output/out]", out.toString());
		//named output
		output = new HamakeParameter("myname=${output}");
		dict.clear();
		dict.put(PathParam.Type.output.toString(), Arrays.asList(new HamakePath("/output/out")));
		out = output.get(dict, FileSystem.get(new Configuration()));
		Assert.assertEquals("[myname=/output/out]", out.toString());
	}
}
