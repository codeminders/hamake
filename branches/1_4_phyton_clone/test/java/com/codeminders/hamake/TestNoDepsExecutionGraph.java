package com.codeminders.hamake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import org.junit.Test;

import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;

public class TestNoDepsExecutionGraph {		

	@Test
	public void testSimpleGraph() throws IOException{		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new HamakePath("I1"), new HamakePath[] {new HamakePath("O1")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new HamakePath("I2"), new HamakePath[] {new HamakePath("O2")});
		MapTask task3 = TestHelperUtils.createMapTask("M3", new HamakePath("O1"), new HamakePath[] {new HamakePath("O3"), new HamakePath("O4")});
		MapTask task4 = TestHelperUtils.createMapTask("M4", new HamakePath("O2"), new HamakePath[] {new HamakePath("O5")});
		MapTask task5 = TestHelperUtils.createMapTask("M5", new HamakePath("O3"), new HamakePath[] {new HamakePath("O6")});
		MapTask task6 = TestHelperUtils.createMapTask("M6", new HamakePath("O4"), new HamakePath[] {new HamakePath("O7")});
		MapTask task7 = TestHelperUtils.createMapTask("M7", new HamakePath("O5"), new HamakePath[] {new HamakePath("O8")});
		ReduceTask task8 = TestHelperUtils.createReduceTask("R1", new HamakePath[] {new HamakePath("O6")}, new HamakePath[] {new HamakePath("O9")});
		ReduceTask task9 = TestHelperUtils.createReduceTask("R2", new HamakePath[] {new HamakePath("O7"), new HamakePath("O8")}, new HamakePath[] {new HamakePath("O9")});
		ArrayList<Task> tasks = new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3, task4, task5, task6, task7, task8, task9}));
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(tasks);
		Assert.assertEquals(2, graph.getReadyForRunTasks().size());
		assertGraphHasLevels(graph, 4);
		//assert no elements left
		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testSimpleGraphWithTargets() throws IOException{		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new HamakePath("I1"), new HamakePath[] {new HamakePath("O1")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new HamakePath("I2"), new HamakePath[] {new HamakePath("O2")});
		MapTask task3 = TestHelperUtils.createMapTask("M3", new HamakePath("I3"), new HamakePath[] {new HamakePath("O3")});
		MapTask task4 = TestHelperUtils.createMapTask("M4", new HamakePath("O1"), new HamakePath[] {new HamakePath("O4")});
		MapTask task5 = TestHelperUtils.createMapTask("M5", new HamakePath("O2"), new HamakePath[] {new HamakePath("O5")});
		MapTask task6 = TestHelperUtils.createMapTask("M6", new HamakePath("O3"), new HamakePath[] {new HamakePath("O6")});
		ReduceTask task7 = TestHelperUtils.createReduceTask("R1", new HamakePath[] {new HamakePath("O4")}, new HamakePath[] {new HamakePath("O7")});
		ReduceTask task8 = TestHelperUtils.createReduceTask("R2", new HamakePath[] {new HamakePath("O5")}, new HamakePath[] {new HamakePath("O8")});
		ReduceTask task9 = TestHelperUtils.createReduceTask("R3", new HamakePath[] {new HamakePath("O6")}, new HamakePath[] {new HamakePath("O9")});
		ArrayList<Task> tasks = new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3, task4, task5, task6, task7, task8, task9}));
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(tasks);
		//Assert that with no targets 3 tasks are ready
		Assert.assertEquals(3, graph.getReadyForRunTasks().size());
		//Assert that still 3 tasks are ready
		Assert.assertEquals(3, graph.getReadyForRunTasks(new String[] {"NOM", "NOR1"}).size());
		Assert.assertEquals(3, graph.getReadyForRunTasks(new String[] {}).size());
		//Assert that 2 task are ready
		Assert.assertEquals(2, graph.getReadyForRunTasks(new String[] {"M5", "R3"}).size());
		//Assert that 1 task is ready
		Assert.assertEquals(1, graph.getReadyForRunTasks(new String[] {"M1", "R1"}).size());
		assertGraphHasLevels(graph, 3);
		//assert no elements left
		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testSimpleCyclicGraph() throws IOException{		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new HamakePath("I1"), new HamakePath[] {new HamakePath("O1")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new HamakePath("O1"), new HamakePath[] {new HamakePath("O2")});
		ReduceTask task3 = TestHelperUtils.createReduceTask("R1", new HamakePath[] {new HamakePath("O2")}, new HamakePath[] {new HamakePath("I1")});
		ReduceTask task4 = TestHelperUtils.createReduceTask("R1", new HamakePath[] {new HamakePath("O2")}, new HamakePath[] {new HamakePath("I1", 1)});
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
	public void testComplexCyclicGraph() throws IOException{		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new HamakePath("I"), new HamakePath[] {new HamakePath("O1")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new HamakePath("O2"), new HamakePath[] {new HamakePath("O3"), new HamakePath("O")});
		MapTask task3 = TestHelperUtils.createMapTask("M3", new HamakePath("O3"), new HamakePath[] {new HamakePath("O4")});
		ReduceTask task4 = TestHelperUtils.createReduceTask("R1", new HamakePath[] {new HamakePath("O1"), new HamakePath("O4")}, new HamakePath[] {new HamakePath("O2")});
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3, task4})));
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());		
		graph.removeTask("M1");
		//assert that cyclic branch won't be executed
		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testCrossDependentGraph() throws IOException{		
		MapTask task1 = TestHelperUtils.createMapTask("M1", new HamakePath("I1"), new HamakePath[] {new HamakePath("O1"), new HamakePath("O3")});
		MapTask task2 = TestHelperUtils.createMapTask("M2", new HamakePath("I2"), new HamakePath[] {new HamakePath("O2"), new HamakePath("O4")});
		ReduceTask task3 = TestHelperUtils.createReduceTask("R1", new HamakePath[] {new HamakePath("O1"), new HamakePath("O4")}, new HamakePath[] {new HamakePath("O5"), new HamakePath("O7")});
		ReduceTask task4 = TestHelperUtils.createReduceTask("R2", new HamakePath[] {new HamakePath("O2"), new HamakePath("O3"), new HamakePath("O7")}, new HamakePath[] {new HamakePath("O6")});
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