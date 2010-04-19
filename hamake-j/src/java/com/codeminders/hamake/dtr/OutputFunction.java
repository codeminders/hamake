package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.List;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.HamakePath;

public abstract class OutputFunction {

	public abstract List<HamakePath> getOutput(HamakePath input, Context context) throws IOException;
}
