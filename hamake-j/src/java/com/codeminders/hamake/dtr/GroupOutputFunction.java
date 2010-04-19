package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.HamakePath;

public class GroupOutputFunction extends OutputFunction{
	
	private List<OutputFunction> outputFunctions = new ArrayList<OutputFunction>();
	
	public void addOutputFunction(OutputFunction outputFunc){
		outputFunctions.add(outputFunc);
	}

	@Override
	public List<HamakePath> getOutput(HamakePath input, Context context)
			throws IOException {
		HashMap<String, HamakePath> set = new HashMap<String, HamakePath>();
		for(OutputFunction function : outputFunctions){
			List<HamakePath> paths = function.getOutput(input, context);
			for(HamakePath path : paths){
				String key = path.getID() + path.getPathName().toString();
				set.put(key, path);
			}
		}
		return new ArrayList<HamakePath>(set.values());
	}
	
	
}
