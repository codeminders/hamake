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
	private List<HamakePath> outputs = new ArrayList<HamakePath>();
	private List<HamakePath> deps = new ArrayList<HamakePath>();

	public String getName() {
		return name;
	}

	public abstract List<HamakePath> getInputs();

	public List<HamakePath> getOutputs() {
		return outputs;
	}

	public boolean dependsOn(Task t) {		
		for (HamakePath i : getInputs()) {
			for (HamakePath o : t.getOutputs()) {
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

	public void setOutputs(List<HamakePath> outputs) {
		this.outputs = outputs;
	}

	public Collection<String> getTaskDeps() {
		return taskDeps;
	}

	public void setTaskDeps(Collection<String> taskDeps) {
		this.taskDeps = taskDeps;
	}
	
	public void setDeps(List<HamakePath> deps) {
        this.deps = deps;
    }
	
	public List<HamakePath> getDeps() {
        return deps;
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
