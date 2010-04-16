package com.codeminders.hamake;

import java.util.*;

class DependencyExecutionGraph extends NoDepsExecutionGraph {

	public DependencyExecutionGraph(List<Task> tasks) {
		super(tasks);
	}

	@Override
	protected boolean dependsOn(Task a, Task b){
		if (a.getTaskDeps().contains(b.getName())) return true;
		return super.dependsOn(a, b);
	}
    
}

