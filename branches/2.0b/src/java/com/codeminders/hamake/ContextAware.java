package com.codeminders.hamake;

public abstract class ContextAware {
	private Context context;
	
	public ContextAware(Context parentContext){
		context = new Context(parentContext);
	}
	
	public Context getContext(){
		return context;
	}
}
	
