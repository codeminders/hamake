package com.codeminders.hamake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import com.codeminders.hamake.dtr.DataTransformationRule;
import com.codeminders.hamake.dtr.Foreach;

public class TestDependencyExecutionGraph {
	
//	@Test
//	public void testGraphWithDependencies() throws IOException{		
//		Foreach task1 = TestHelperUtils.createMapTask("M1", new HamakePath("I1"), new HamakePath[]{});
//		Foreach task2 = TestHelperUtils.createMapTaskWithTaskDeps("M2", new HamakePath("I2"), new HamakePath[] {}, new String[]{"M1"});
//		DependencyExecutionGraph graph = new DependencyExecutionGraph(new ArrayList<DataTransformationRule>(Arrays.asList(new DataTransformationRule[] {task1, task2})));
//		//assert that M2 is not ready because M2 depends on M1
//		Assert.assertEquals(1, graph.getReadyForRunTasks().size());
//		graph.removeTask("M1");		
//		//assert that M2 is now ready
//		Assert.assertEquals(1, graph.getReadyForRunTasks().size());					
//		graph.removeTask("M2");		
//		//assert that no tasks are left
//		Assert.assertEquals(0, graph.getReadyForRunTasks().size());
//	}
	
}
