package com.codeminders.hamake.context;


public abstract class ContextAware {
	private Context context;
	
	public ContextAware(Context parentContext){
		context = new Context(parentContext);
	}
	
	public Context getContext(){
		return context;
	}
}
	
