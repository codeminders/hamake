package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.codeminders.hamake.InvalidContextStateException;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.data.FileDataFunction;
import com.codeminders.hamake.task.Exec;

public class TestForeach {
	
	@Test
	public void testEmptyInputDoesNotPreventDAGExecution() throws InvalidContextStateException, IOException{
		DataFunction df = new FileDataFunction("1", 0, Long.MAX_VALUE, null, "/dummyPath"){
			@Override
			public List<Path> getPath(Context context) {
				return Collections.emptyList();
			}
		};
		Foreach foreach = new Foreach(new Context(null), df , Arrays.asList(new DataFunction[]{df}), null);
		Semaphore semaphore = new Semaphore(1);
		foreach.setTask(new Exec());
		Assert.assertEquals(0, foreach.execute(semaphore));
	}

}
