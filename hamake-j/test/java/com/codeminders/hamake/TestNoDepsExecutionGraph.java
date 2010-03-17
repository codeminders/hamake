package com.codeminders.hamake;

import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.Assert;
import org.junit.Test;

import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;

public class TestNoDepsExecutionGraph {
	
	private MapTask createMapTask(String name, Path input, Path[] outputs){
		MapTask map = new MapTask();
		map.setName(name);
		map.setXinput(input);
		map.setOutputs(Arrays.asList(outputs));
		return map;
	}
	
	private ReduceTask createReduceTask(String name, Path[] inputs, Path[] outputs){
		ReduceTask reduce = new ReduceTask();
		reduce.setName(name);
		reduce.setInputs(Arrays.asList(inputs));
		reduce.setOutputs(Arrays.asList(outputs));
		return reduce;
	}

	@Test
	public void testBuildSimpleGraph(){		
		MapTask task1 = createMapTask("M1", new Path("I1"), new Path[] {new Path("O1")});
		MapTask task2 = createMapTask("M2", new Path("I2"), new Path[] {new Path("O2")});
		MapTask task3 = createMapTask("M3", new Path("O1"), new Path[] {new Path("O3"), new Path("O4")});
		MapTask task4 = createMapTask("M4", new Path("O2"), new Path[] {new Path("O5")});
		MapTask task5 = createMapTask("M5", new Path("O3"), new Path[] {new Path("O6")});
		MapTask task6 = createMapTask("M6", new Path("O4"), new Path[] {new Path("O7")});
		MapTask task7 = createMapTask("M7", new Path("O5"), new Path[] {new Path("O8")});
		ReduceTask task8 = createReduceTask("R1", new Path[] {new Path("O6")}, new Path[] {new Path("O9")});
		ReduceTask task9 = createReduceTask("R1", new Path[] {new Path("O7"), new Path("O8")}, new Path[] {new Path("O9")});
		ArrayList<Task> tasks = new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3, task4, task5, task6, task7, task8, task9}));
		NoDepsExecutionGraph graph = new NoDepsExecutionGraph(tasks);
		Assert.assertEquals(2, graph.getReadyForRunTasks().size());
	}
	
	@Test
	public void testBuildCyclicGraph(){		
		MapTask task1 = createMapTask("M1", new Path("I1"), new Path[] {new Path("O1")});
		MapTask task2 = createMapTask("M2", new Path("O1"), new Path[] {new Path("O2")});
		ReduceTask task3 = createReduceTask("R1", new Path[] {new Path("O2")}, new Path[] {new Path("I1")});
		ReduceTask task4 = createReduceTask("R1", new Path[] {new Path("O2")}, new Path[] {new Path("I1", 1)});
		NoDepsExecutionGraph graph1 = new NoDepsExecutionGraph(new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task3})));
		Assert.assertEquals(0, graph1.getReadyForRunTasks().size());
		NoDepsExecutionGraph graph2 = new NoDepsExecutionGraph(new ArrayList<Task>(Arrays.asList(new Task[] {task1, task2, task4})));
		Assert.assertEquals(1, graph2.getReadyForRunTasks().size());
	}
}
