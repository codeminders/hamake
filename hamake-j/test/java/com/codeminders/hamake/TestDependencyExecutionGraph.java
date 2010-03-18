package com.codeminders.hamake;

import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.utils.TestHelperUtils;

public class TestDependencyExecutionGraph {
	
	@Test
	public void testGraphWithDependencies(){		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new Path("I1"), new Path[]{});
		MapTask task2 = TestHelperUtils.createMapTaskWithTaskDeps("M2", new Path("I2"), new Path[] {}, new String[]{"M1"});
		DependencyExecutionGraph graph = new DependencyExecutionGraph(new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2})));
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());
		graph.removeTask("M1");		
		//assert that R2 is not ready because R1 and M2 are still running
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());					
		graph.removeTask("M2");		
		//assert that R2 is not ready because R1 is still running
		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
	}
	
}
