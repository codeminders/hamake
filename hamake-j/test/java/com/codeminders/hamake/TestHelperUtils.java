package com.codeminders.hamake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;

import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.data.FileDataFunction;
import com.codeminders.hamake.dtr.DataTransformationRule;
import com.codeminders.hamake.dtr.Fold;
import com.codeminders.hamake.dtr.Foreach;
import com.codeminders.hamake.task.Exec;


public class TestHelperUtils {
	
	public static String generateTemporaryPath(String folder, String prefix){
		Random rand = new Random();
		String tempDir = StringUtils.isEmpty(folder) ? System.getProperty("java.io.tmpdir", "/tmp") : folder ;
		String tempFileName = prefix + "-" + Math.abs(rand.nextInt() % 100000);
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
		return generateTemporaryFile(folder, null);
	}
	
	public static File generateTemporaryFile(String folder, String extention) throws IOException{
		if(!new File(folder).exists()){
			new File(folder).mkdirs();
		}
		String path = generateTemporaryPath(folder, "file") + (StringUtils.isEmpty(extention)? "" : extention);
		File f = new File(path);
		f.createNewFile();
		return f;
	}
	
	public static File[] generateTemporaryFiles(String path, int amount) throws IOException{
		return generateTemporaryFiles(path, amount, null);
	}

	public static File[] generateTemporaryFiles(String path, int amount, String extention) throws IOException{
		File dir = new File(path);
		if(!dir.exists() || !dir.isDirectory()){
			dir.mkdirs();
		}		
		List<File> files = new ArrayList<File>();
		for(int i = 0; i < amount; i++){
			files.add(generateTemporaryFile(path, extention));
		}
		return files.toArray(new File[] {});
	}
	
//	public static void setMapTaskInputOutputFolders(Hamake make, String taskName, HamakePath inputFolder, HamakePath outputFolder){
//		Collection<DataTransformationRule> tasks = make.getTasks();
//		Iterator<DataTransformationRule> i = tasks.iterator();
//		while(i.hasNext()){
//			DataTransformationRule task = i.next();
//			if(task instanceof Foreach){
//				Foreach m = (Foreach)task;
//				if(task.getName().equals(taskName)){
//					m.setXinput(inputFolder);
//					m.getOutputs().clear();
//					m.getOutputs().add(outputFolder);
//				}
//			}
//		}
//	}
	
//	public static void setReduceTaskInputOutputFolders(Hamake make, String taskName, HamakePath inputFolder, HamakePath outputFolder){
//		Collection<DataTransformationRule> tasks = make.getTasks();
//		Iterator<DataTransformationRule> i = tasks.iterator();
//		while(i.hasNext()){
//			DataTransformationRule task = i.next();			
//			if(task instanceof Fold){
//				Fold r = (Fold)task;
//				if(task.getName().equals(taskName)){
//					List<HamakePath> inputs = r.getInputs();
//					inputs.clear();
//					inputs.add(inputFolder);
//					r.setInputs(inputs);
//					r.getOutputs().clear();
//					r.getOutputs().add(outputFolder);
//				}				
//			}
//		}
//	}
	
//	public static void setTaskExecBinary(Hamake make, String taskName, String binary) throws IOException{
//		Collection<DataTransformationRule> tasks = make.getTasks();
//		Iterator<DataTransformationRule> i = tasks.iterator();
//		while(i.hasNext()){
//			DataTransformationRule task = i.next();
//			if(task instanceof Foreach){				
//				Foreach m = (Foreach)task;
//				if(task.getName().equals(taskName)){
//					Exec command = (Exec)m.getCommand();
//					command.setBinary(new HamakePath(binary));
//				}
//			}
//			if(task instanceof Fold){
//				Fold r = (Fold)task;
//				if(task.getName().equals(taskName)){
//					Exec command = (Exec)r.getCommand();
//					command.setBinary(new HamakePath(binary));
//				}				
//			}
//		}
//	}
	
	public static Foreach createForeachDTR(Context context, String name, FileDataFunction input, List<? extends DataFunction> output){
		Foreach foreach = new Foreach(context, input, output, null);
		foreach.setName(name);
		return foreach;
	}
	
	public static Fold createFoldDTR(Context context, String name, List<? extends DataFunction> inputs, List<? extends DataFunction> outputs){
		Fold fold = new Fold(context, inputs, outputs, null);
		fold.setName(name);
		return fold;
	}
	
//	public static Foreach createMapTaskWithTaskDeps(String name, HamakePath input, HamakePath[] outputs, String[] dependsOns){
//		Foreach map = new Foreach();
//		map.setName(name);
//		map.setXinput(input);
//		map.setOutputs(Arrays.asList(outputs));
//		map.setTaskDeps(Arrays.asList(dependsOns));
//		return map;
//		return null;
//	}		
	
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
