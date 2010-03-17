package com.codeminders.hamake.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;

import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.OS;
import com.codeminders.hamake.Path;
import com.codeminders.hamake.Task;
import com.codeminders.hamake.commands.ExecCommand;
import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;


public class TestHelperUtils {
	
	public static String generateTemporaryPath(String folder, String prefix){
		Random rand = new Random();
		String tempDir = StringUtils.isEmpty(folder) ? System.getProperty("java.io.tmpdir", "/tmp") : folder ;
		String tempFileName = prefix + "-" + Math.abs(rand.nextInt() % 1000);
		return tempDir + File.separator + tempFileName;
	}
	
	public static File generateTemporaryDirectory(String folder, String prefix){
		File f = new File(generateTemporaryPath(folder, (StringUtils.isEmpty(prefix)? "" : prefix) + (StringUtils.isEmpty(prefix)? "" : "-") + "folder"));
		f.mkdirs();
		return f;
	}
	
	public static File generateTemporaryDirectory(String folder){					
		return generateTemporaryDirectory(folder, null);
	}
	
	public static File generateTemporaryDirectory(){					
		return generateTemporaryDirectory(null, null);
	}
	
	public static File generateTemporaryFile(String folder) throws IOException{
		if(!new File(folder).exists()){
			new File(folder).mkdirs();
		}
		File f = new File(generateTemporaryPath(folder, "file"));
		f.createNewFile();
		return f;
	}

	public static File[] generateTemporaryFiles(String path, int amount) throws IOException{
		File dir = new File(path);
		if(!dir.exists() || !dir.isDirectory()){
			dir.mkdirs();
		}		
		List<File> files = new ArrayList<File>();
		for(int i = 0; i < amount; i++){
			files.add(generateTemporaryFile(path));
		}
		return files.toArray(new File[] {});
	}
	
	public static void setMapTaskInputOutputFolders(Hamake make, String taskName, Path inputFolder, Path outputFolder){
		Collection<Task> tasks = make.getTasks();
		Iterator<Task> i = tasks.iterator();
		while(i.hasNext()){
			Task task = i.next();
			if(task instanceof MapTask){
				MapTask m = (MapTask)task;
				if(task.getName().equals(taskName)){
					m.setXinput(inputFolder);
					m.getOutputs().clear();
					m.getOutputs().add(outputFolder);
				}
			}
		}
	}
	
	public static void setReduceTaskInputOutputFolders(Hamake make, String taskName, Path inputFolder, Path outputFolder){
		Collection<Task> tasks = make.getTasks();
		Iterator<Task> i = tasks.iterator();
		while(i.hasNext()){
			Task task = i.next();			
			if(task instanceof ReduceTask){
				ReduceTask r = (ReduceTask)task;
				if(task.getName().equals(taskName)){
					List<Path> inputs = r.getInputs();
					inputs.clear();
					inputs.add(outputFolder);
					r.setInputs(inputs);
					r.getOutputs().clear();
					r.getOutputs().add(inputFolder);
				}				
			}
		}
	}
	
	public static void setTaskExecBinary(Hamake make, String taskName, String binary){
		Collection<Task> tasks = make.getTasks();
		Iterator<Task> i = tasks.iterator();
		while(i.hasNext()){
			Task task = i.next();
			if(task instanceof MapTask){				
				MapTask m = (MapTask)task;
				if(task.getName().equals(taskName)){
					ExecCommand command = (ExecCommand)m.getCommand();
					command.setBinary(binary);
				}
			}
			if(task instanceof ReduceTask){
				ReduceTask r = (ReduceTask)task;
				if(task.getName().equals(taskName)){
					ExecCommand command = (ExecCommand)r.getCommand();
					command.setBinary(binary);
				}				
			}
		}
	}
}
