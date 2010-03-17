package com.codeminders.hamake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.builder.ToStringBuilder;

public abstract class Task {

	private String name;
	private Collection<String> taskDeps = new ArrayList<String>();
	private Command command;
	private List<Path> outputs = new ArrayList<Path>();

	public String getName() {
		return name;
	}

	public abstract List<Path> getInputs();

	public List<Path> getOutputs() {
		return outputs;
	}

	public boolean dependsOn(Task t) {
		if (getTaskDeps().contains(t.getName()))
			return true;
		for (Path i : getInputs()) {
			for (Path o : t.getOutputs()) {
				if (i.intersects(o))
					return true;
			}
		}

		return false;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Command getCommand() {
		return command;
	}

	public void setCommand(Command command) {
		this.command = command;
	}

	public void setOutputs(List<Path> outputs) {
		this.outputs = outputs;
	}

	public Collection<String> getTaskDeps() {
		return taskDeps;
	}

	public void setTaskDeps(Collection<String> taskDeps) {
		this.taskDeps = taskDeps;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("name", name).append("command",
				command).append("outputs", outputs)
				.append("taskdeps", taskDeps).toString();
	}

	public abstract int execute(Semaphore semaphore, Map<String, Object> context)
			throws IOException;		

}
