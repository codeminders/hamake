package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.context.ContextAware;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.task.Task;

public abstract class DataTransformationRule extends ContextAware{

	private String name;
	private Task task;
	
	public DataTransformationRule(Context parentContext){
		super(parentContext);
	}
	
	protected abstract List<? extends DataFunction> getInputs();
	
	protected abstract List<? extends DataFunction> getOutputs();
	
	protected abstract List<? extends DataFunction> getDeps();
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public boolean dependsOn(DataTransformationRule t) throws IOException {		
		for (DataFunction i : getInputs()) {
			for (DataFunction o : t.getOutputs()) {
				if (i.intersects(getContext(), o))
					return true;
			}
		}
		return false;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}

	@Override
	public String toString() {
		return getName();
	}
	
	public int executeIfCan(Semaphore semaphore) throws IOException{
		boolean canStart = true;
		if((Boolean)getContext().get(Context.HAMAKE_PROPERTY_WITH_DEPENDENCIES)){
			for(DataFunction depDataFunc : getDeps()){
				for(Path path : depDataFunc.getPath(getContext())){
					FileSystem fs = depDataFunc.getFileSystem(getContext(), path);
					if(!fs.exists(path)){
						Log.warn("DTR " + getName() + " depends on " + path + ". DTR will not execute");
						canStart = false;
					}
				}
			}
		}
		return canStart? execute(semaphore) : 0;
	}

	protected abstract int execute(Semaphore semaphore)
			throws IOException;		
	
}