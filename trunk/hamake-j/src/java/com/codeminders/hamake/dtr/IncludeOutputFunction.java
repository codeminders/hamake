package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.HamakePath;

public class IncludeOutputFunction extends OutputFunction{
	
	private String reference;
	
	public IncludeOutputFunction(String reference){
		this.reference = reference;
	}

	@Override
	public List<HamakePath> getOutput(HamakePath input, Context context)
			throws IOException {
		return Arrays.asList((HamakePath)context.get(reference));
	}

}
