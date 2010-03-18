package com.codeminders.hamake;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import org.junit.Test;

import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;
import com.codeminders.hamake.utils.TestHelperUtils;

public class TestNoDepsExecutionGraph {		

	@Test
	public void testSimpleGraph(){		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new Path("I1"), new Path[] {new Path("O1")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new Path("I2"), new Path[] {new Path("O2")});
		MapTask task3 = TestHelperUtils.createMapTask("M3", new Path("O1"), new Path[] {new Path("O3"), new Path("O4")});
		MapTask task4 = TestHelperUtils.createMapTask("M4", new Path("O2"), new Path[] {new Path("O5")});
		MapTask task5 = TestHelperUtils.createMapTask("M5", new Path("O3"), new Path[] {new Path("O6")});
		MapTask task6 = TestHelperUtils.createMapTask("M6", new Path("O4"), new Path[] {new Path("O7")});
		MapTask task7 = TestHelperUtils.createMapTask("M7", new Path("O5"), new Path[] {new Path("O8")});
		ReduceTask task8 = TestHelperUtils.createReduceTask("R1", new Path[] {new Path("O6")}, new Path[] {new Path("O9")});
		ReduceTask task9 = TestHelperUtils.createReduceTask("R2", new Path[] {new Path("O7"), new Path("O8")}, new Path[] {new Path("O9")});
		ArrayList<Task> tasks = new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3, task4, task5, task6, task7, task8, task9}));
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(tasks);
		Assert.assertEquals(2, graph.getReadyForRunTasks().size());
		assertGraphHasLevels(graph, 4);
		//assert no elements left
		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testSimpleCyclicGraph(){		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new Path("I1"), new Path[] {new Path("O1")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new Path("O1"), new Path[] {new Path("O2")});
		ReduceTask task3 = TestHelperUtils.createReduceTask("R1", new Path[] {new Path("O2")}, new Path[] {new Path("I1")});
		ReduceTask task4 = TestHelperUtils.createReduceTask("R1", new Path[] {new Path("O2")}, new Path[] {new Path("I1", 1)});
		NoDepsExecutionGraph graph1 = new NoDepsExecutionGraph(new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3})));
		//assert that cycle has been detected
		Assert.assertEquals(0, graph1.getReadyForRunTasks().size());
		NoDepsExecutionGraph graph2 = new NoDepsExecutionGraph(new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task4})));
		//assert that generations work
		Assert.assertEquals(1, graph2.getReadyForRunTasks().size());
		assertGraphHasLevels(graph2, 3);	
		//assert no elements left
		Assert.assertEquals(0, graph2.getReadyForRunTasks().size());
	}	
		
	@Test
	public void testComplexCyclicGraph(){		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new Path("I"), new Path[] {new Path("O1")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new Path("O2"), new Path[] {new Path("O3"), new Path("O")});
		MapTask task3 = TestHelperUtils.createMapTask("M3", new Path("O3"), new Path[] {new Path("O4")});
		ReduceTask task4 = TestHelperUtils.createReduceTask("R1", new Path[] {new Path("O1"), new Path("O4")}, new Path[] {new Path("O2")});
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3, task4})));
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());		
		graph.removeTask("M1");
		//assert that cyclic branch won't be executed
		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testCrossDependentGraph(){		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new Path("I1"), new Path[] {new Path("O1"), new Path("O3")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new Path("I2"), new Path[] {new Path("O2"), new Path("O4")});
		ReduceTask task3 = TestHelperUtils.createReduceTask("R1", new Path[] {new Path("O1"), new Path("O4")}, new Path[] {new Path("O5"), new Path("O7")});
		ReduceTask task4 = TestHelperUtils.createReduceTask("R2", new Path[] {new Path("O2"), new Path("O3"), new Path("O7")}, new Path[] {new Path("O6")});
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3, task4})));
		Assert.assertEquals(2, graph.getReadyForRunTasks().size());
		graph.removeTask("M1");		
		//assert that R2 is not ready because R1 and M2 are still running
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());					
		graph.removeTask("M2");		
		//assert that R2 is not ready because R1 is still running
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());
	}				
	
	private void assertGraphHasLevels(ExecutionGraph graph, int expectedSteps){		
		int actualSteps = 0;
		List<String> tasks = null;
		do{
			tasks = graph.getReadyForRunTasks();
			if(tasks.size() > 0){
				for(String task : tasks){
					graph.removeTask(task);
				}
				actualSteps++;
			} 
		}while(tasks.size() > 0);
		Assert.assertEquals("Graph level", expectedSteps, actualSteps);
	}
}
