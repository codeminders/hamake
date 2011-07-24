package com.codeminders.hamake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.FileDataFunction;
import com.codeminders.hamake.dtr.DataTransformationRule;
import com.codeminders.hamake.dtr.Fold;
import com.codeminders.hamake.dtr.Foreach;

public class TestNoDepsExecutionGraph {		

	@Test
	public void testSimpleGraph() throws IOException, InvalidContextStateException{
		Context context = new Context(new Configuration(), null, false, false, false);
		Foreach task1 = HelperUtils.createForeachDTR(context, "M1", new FileDataFunction("I1"), Arrays.asList(new FileDataFunction("O1")));
		Foreach task2 = HelperUtils.createForeachDTR(context, "M2", new FileDataFunction("I2"), Arrays.asList(new FileDataFunction("O2")));
		Foreach task3 = HelperUtils.createForeachDTR(context, "M3", new FileDataFunction("O1"), Arrays.asList(new FileDataFunction("O3"), new FileDataFunction("O4")));
		Foreach task4 = HelperUtils.createForeachDTR(context, "M4", new FileDataFunction("O2"), Arrays.asList(new FileDataFunction("O5")));
		Foreach task5 = HelperUtils.createForeachDTR(context, "M5", new FileDataFunction("O3"), Arrays.asList(new FileDataFunction("O6")));
		Foreach task6 = HelperUtils.createForeachDTR(context, "M6", new FileDataFunction("O4"), Arrays.asList(new FileDataFunction("O7")));
		Foreach task7 = HelperUtils.createForeachDTR(context, "M7", new FileDataFunction("O5"), Arrays.asList(new FileDataFunction("O8")));
		Fold task8 = HelperUtils.createFoldDTR(context, "R1", Arrays.asList(new FileDataFunction("O6")), Arrays.asList(new FileDataFunction("O9")));
		Fold task9 = HelperUtils.createFoldDTR(context, "R2", Arrays.asList(new FileDataFunction("O7"), new FileDataFunction("O8")), Arrays.asList(new FileDataFunction("O9")));
		ArrayList<DataTransformationRule> tasks = new ArrayList<DataTransformationRule>(Arrays.asList(new DataTransformationRule[] {task1, task2, task3, task4, task5, task6, task7, task8, task9}));
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(tasks);
		Assert.assertEquals(2, graph.getReadyForRunTasks().size());
		assertGraphHasLevels(graph, 4);
		//assert no elements left
		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testSimpleGraphWithTargets() throws IOException, InvalidContextStateException{
		Context context = new Context(new Configuration(), null, false, false, false);
		Foreach task1 = HelperUtils.createForeachDTR(context, "M1", new FileDataFunction("I1"), Arrays.asList(new FileDataFunction("O1")));
		Foreach task2 = HelperUtils.createForeachDTR(context, "M2", new FileDataFunction("I2"), Arrays.asList(new FileDataFunction("O2")));
		Foreach task3 = HelperUtils.createForeachDTR(context, "M3", new FileDataFunction("I3"), Arrays.asList(new FileDataFunction("O3")));
		Foreach task4 = HelperUtils.createForeachDTR(context, "M4", new FileDataFunction("O1"), Arrays.asList(new FileDataFunction("O4")));
		Foreach task5 = HelperUtils.createForeachDTR(context, "M5", new FileDataFunction("O2"), Arrays.asList(new FileDataFunction("O5")));
		Foreach task6 = HelperUtils.createForeachDTR(context, "M6", new FileDataFunction("O3"), Arrays.asList(new FileDataFunction("O6")));
		Fold task7 = HelperUtils.createFoldDTR(context, "R1", Arrays.asList(new FileDataFunction("O4")), Arrays.asList(new FileDataFunction("O7")));
		Fold task8 = HelperUtils.createFoldDTR(context, "R2", Arrays.asList(new FileDataFunction("O5")), Arrays.asList(new FileDataFunction("O8")));
		Fold task9 = HelperUtils.createFoldDTR(context, "R3", Arrays.asList(new FileDataFunction("O6")), Arrays.asList(new FileDataFunction("O9")));
		ArrayList<DataTransformationRule> tasks = new ArrayList<DataTransformationRule>(Arrays.asList(new DataTransformationRule[] {task1, task2, task3, task4, task5, task6, task7, task8, task9}));
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
	public void testSimpleCyclicGraph() throws IOException, InvalidContextStateException{	
		Context context = new Context(new Configuration(), null, false, false, false);
		Foreach task1 = HelperUtils.createForeachDTR(context, "M1", new FileDataFunction("I1"), Arrays.asList(new FileDataFunction("O1")));
		Foreach task2 = HelperUtils.createForeachDTR(context, "M2", new FileDataFunction("O1"), Arrays.asList(new FileDataFunction("O2")));
		Fold task3 = HelperUtils.createFoldDTR(context, "R1", Arrays.asList(new FileDataFunction("O2")), Arrays.asList(new FileDataFunction("I1")));
		Fold task4 = HelperUtils.createFoldDTR(context, "R1", Arrays.asList(new FileDataFunction("O2")), Arrays.asList(new FileDataFunction("I1", 1)));
		NoDepsExecutionGraph graph1 = new NoDepsExecutionGraph(new ArrayList<DataTransformationRule>(Arrays.asList(new DataTransformationRule[] {task1, task2, task3})));
		//assert that cycle has been detected
		Assert.assertEquals(0, graph1.getReadyForRunTasks().size());
		NoDepsExecutionGraph graph2 = new NoDepsExecutionGraph(new ArrayList<DataTransformationRule>(Arrays.asList(new DataTransformationRule[] {task1, task2, task4})));
		//assert that generations work
		Assert.assertEquals(1, graph2.getReadyForRunTasks().size());
		assertGraphHasLevels(graph2, 3);	
		//assert no elements left
		Assert.assertEquals(0, graph2.getReadyForRunTasks().size());
	}	
		
	@Test
	public void testComplexCyclicGraph() throws IOException, InvalidContextStateException{	
		Context context = new Context(new Configuration(), null, false, false, false);
		Foreach task1 = HelperUtils.createForeachDTR(context, "M1", new FileDataFunction("I"), Arrays.asList(new FileDataFunction("O1")));
		Foreach task2 = HelperUtils.createForeachDTR(context, "M2", new FileDataFunction("O2"), Arrays.asList(new FileDataFunction("O3"), new FileDataFunction("O")));
		Foreach task3 = HelperUtils.createForeachDTR(context, "M3", new FileDataFunction("O3"), Arrays.asList(new FileDataFunction("O4")));
		Fold task4 = HelperUtils.createFoldDTR(context, "R1", Arrays.asList(new FileDataFunction("O1"), new FileDataFunction("O4")), Arrays.asList(new FileDataFunction("O2")));
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(new ArrayList<DataTransformationRule>(Arrays.asList(new DataTransformationRule[] {task1, task2, task3, task4})));
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());		
		graph.removeTask("M1");
		//assert that cyclic branch won't be executed
		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testCrossDependentGraph() throws IOException, InvalidContextStateException{
		Context context = new Context(new Configuration(), null, false, false, false);
		Foreach task1 = HelperUtils.createForeachDTR(context, "M1", new FileDataFunction("I1"), Arrays.asList(new FileDataFunction("O1"), new FileDataFunction("O3")));
		Foreach task2 = HelperUtils.createForeachDTR(context, "M2", new FileDataFunction("I2"), Arrays.asList(new FileDataFunction("O2"), new FileDataFunction("O4")));
		Fold task3 = HelperUtils.createFoldDTR(context, "R1", Arrays.asList(new FileDataFunction("O1"), new FileDataFunction("O4")), Arrays.asList(new FileDataFunction("O5"), new FileDataFunction("O7")));
		Fold task4 = HelperUtils.createFoldDTR(context, "R2", Arrays.asList(new FileDataFunction("O2"), new FileDataFunction("O3"), new FileDataFunction("O7")), Arrays.asList(new FileDataFunction("O6")));
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(new ArrayList<DataTransformationRule>(Arrays.asList(new DataTransformationRule[] {task1, task2, task3, task4})));
		Assert.assertEquals(2, graph.getReadyForRunTasks().size());
		graph.removeTask("M1");		
		//assert that R2 is not ready because R1 and M2 are still running
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());					
		graph.removeTask("M2");		
		//assert that R2 is not ready because R1 is still running
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testGraphWithDublicatedDependencies() throws IOException, InvalidContextStateException{
		Context context = new Context(new Configuration(), null, false, false, false);
		Fold taskA = HelperUtils.createFoldDTR(context, "a", Arrays.asList(new FileDataFunction("inputA")), Arrays.asList(new FileDataFunction("outputA")));
		Fold taskB = HelperUtils.createFoldDTR(context, "b", Arrays.asList(new FileDataFunction("inputB")), Arrays.asList(new FileDataFunction("outputB")));
		Fold taskD = HelperUtils.createFoldDTR(context, "d", Arrays.asList(new FileDataFunction("outputA"), new FileDataFunction("outputB")), Arrays.asList(new FileDataFunction("outputD")));
		Fold taskC = HelperUtils.createFoldDTR(context, "c", Arrays.asList(new FileDataFunction("outputA"), new FileDataFunction("outputB")), Arrays.asList(new FileDataFunction("outputC")));
		//order in which tasks are added is important here
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(new ArrayList<DataTransformationRule>(Arrays.asList(new DataTransformationRule[] {taskA, taskB, taskD, taskC})));
		Assert.assertEquals(2, graph.getReadyForRunTasks().size());
		graph.removeTask("a");		
		Assert.assertEquals(1, graph.getReadyForRunTasks().size());					
		graph.removeTask("b");		
		//assert that d and c are now ready
		Assert.assertEquals(2, graph.getReadyForRunTasks().size());
	}
	
	private void assertGraphHasLevels(ExecutionGraph graph, int expectedSteps){		
		int actualSteps = 0;
		Set<String> tasks = null;
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
