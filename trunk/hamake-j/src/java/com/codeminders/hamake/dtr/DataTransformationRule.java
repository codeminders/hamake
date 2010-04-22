package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.task.Task;

public abstract class DataTransformationRule {

	private String name;
	private Task task;
	
	protected abstract List<? extends DataFunction> getInputs();
	
	protected abstract List<? extends DataFunction> getOutputs();
	
	protected abstract List<? extends DataFunction> getDeps();
	
	protected abstract Context getContext();
	
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
		return new ToStringBuilder(this).append("name", name).append("task",
				task).append("outputs", getOutputs())
				.append("deps", getDeps()).toString();
	}

	public abstract int execute(Semaphore semaphore)
			throws IOException;		

}
