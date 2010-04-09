package com.codeminders.hamake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.ranges.RangeException;

import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.OS;
import com.codeminders.hamake.HamakePath;
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
	
	public static void setMapTaskInputOutputFolders(Hamake make, String taskName, HamakePath inputFolder, HamakePath outputFolder){
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
	
	public static void setReduceTaskInputOutputFolders(Hamake make, String taskName, HamakePath inputFolder, HamakePath outputFolder){
		Collection<Task> tasks = make.getTasks();
		Iterator<Task> i = tasks.iterator();
		while(i.hasNext()){
			Task task = i.next();			
			if(task instanceof ReduceTask){
				ReduceTask r = (ReduceTask)task;
				if(task.getName().equals(taskName)){
					List<HamakePath> inputs = r.getInputs();
					inputs.clear();
					inputs.add(inputFolder);
					r.setInputs(inputs);
					r.getOutputs().clear();
					r.getOutputs().add(outputFolder);
				}				
			}
		}
	}
	
	public static void setTaskExecBinary(Hamake make, String taskName, String binary) throws IOException{
		Collection<Task> tasks = make.getTasks();
		Iterator<Task> i = tasks.iterator();
		while(i.hasNext()){
			Task task = i.next();
			if(task instanceof MapTask){				
				MapTask m = (MapTask)task;
				if(task.getName().equals(taskName)){
					ExecCommand command = (ExecCommand)m.getCommand();
					command.setBinary(new HamakePath(binary));
				}
			}
			if(task instanceof ReduceTask){
				ReduceTask r = (ReduceTask)task;
				if(task.getName().equals(taskName)){
					ExecCommand command = (ExecCommand)r.getCommand();
					command.setBinary(new HamakePath(binary));
				}				
			}
		}
	}
	
	public static MapTask createMapTask(String name, HamakePath input, HamakePath[] outputs){
		MapTask map = new MapTask();
		map.setName(name);
		map.setXinput(input);
		map.setOutputs(Arrays.asList(outputs));
		return map;
	}
	
	public static ReduceTask createReduceTask(String name, HamakePath[] inputs, HamakePath[] outputs){
		ReduceTask reduce = new ReduceTask();
		reduce.setName(name);
		reduce.setInputs(Arrays.asList(inputs));
		reduce.setOutputs(Arrays.asList(outputs));
		return reduce;
	}
	
	public static MapTask createMapTaskWithTaskDeps(String name, HamakePath input, HamakePath[] outputs, String[] dependsOns){
		MapTask map = new MapTask();
		map.setName(name);
		map.setXinput(input);
		map.setOutputs(Arrays.asList(outputs));
		map.setTaskDeps(Arrays.asList(dependsOns));
		return map;
	}		
	
	public static File getExamplesJar() throws IOException{
		String examplesJar = System.getProperty("examples.jar");
		File f = new File(examplesJar);
		if(StringUtils.isEmpty(examplesJar) || !f.exists() || !f.isFile()){
			throw new IOException("examples jar " + examplesJar + " is not found or is not a file");
		}
		return f;
	}
	
	public static File getHamakefilesDir() throws IOException{
		String hamakefilesDir = System.getProperty("hamakefiles.dir");
		File f = new File(hamakefilesDir);
		if(StringUtils.isEmpty(hamakefilesDir) || !f.exists() || !f.isDirectory()){
			throw new IOException("hamakefiles dir " + hamakefilesDir + " is not found or is not a folder");
		}
		return f;
	}
}
