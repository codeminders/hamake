package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.Utils;

public class IdentityOutputFunction extends OutputFunction {

	private String outputValue;
	private Long validityPeriod;
	
	public IdentityOutputFunction(String functionString){
		this(functionString, Long.MAX_VALUE);
	}

	public IdentityOutputFunction(String functionString, long validityPeriod) {
		this.outputValue = functionString;
		this.validityPeriod = validityPeriod;
	}
	
	@Override
	public List<HamakePath> getOutput(HamakePath input, Context context)
			throws IOException {
		return Arrays.asList(new HamakePath(input.getID() + "_out", input.getWdir(),
				Utils.replaceVariables(context, outputValue), null,
						null, input.getGen(), input.getVariant(), validityPeriod));
	}

}
