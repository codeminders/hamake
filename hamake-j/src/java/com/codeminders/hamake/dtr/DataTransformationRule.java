package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.task.Task;

public abstract class DataTransformationRule {

	private String name;
	private Task task;
	
	protected abstract List<HamakePath> getInputs();
	
	protected abstract List<HamakePath> getOutputs();
	
	protected abstract List<HamakePath> getDeps();
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public boolean dependsOn(DataTransformationRule t) {		
		for (HamakePath i : getInputs()) {
			for (HamakePath o : t.getOutputs()) {
				if (i.intersects(o))
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
