package com.codeminders.hamake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.data.FileDataFunction;
import com.codeminders.hamake.dtr.Fold;
import com.codeminders.hamake.dtr.Foreach;


public class HelperUtils {
	
	public static String generateTemporaryPath(String folder, String prefix){
		String tempDir = StringUtils.isEmpty(folder) ? System.getProperty("java.io.tmpdir", "/tmp") : folder ;
		String tempFileName = prefix + "-" + UUID.randomUUID().toString();
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
	
	public static Foreach createForeachDTR(Context context, String name, FileDataFunction input, List<? extends DataFunction> output) throws InvalidContextStateException{
		Foreach foreach = new Foreach(context, input, output, null);
		foreach.setName(name);
		return foreach;
	}
	public static Foreach createForeachDTR(Context context, String name, FileDataFunction input, List<? extends DataFunction> output, List<? extends DataFunction> deps) throws InvalidContextStateException{
		Foreach foreach = new Foreach(context, input, output, deps);
		foreach.setName(name);
		return foreach;
	}
	
	public static Fold createFoldDTR(Context context, String name, List<? extends DataFunction> inputs, List<? extends DataFunction> outputs) throws InvalidContextStateException{
		Fold fold = new Fold(context, inputs, outputs, null);
		fold.setName(name);
		return fold;
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
