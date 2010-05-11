package com.codeminders.hamake.params;

import java.io.IOException;

import com.codeminders.hamake.context.Context;

public interface Parameter {
	
	public abstract String get(Context context) throws IOException;
	
}
