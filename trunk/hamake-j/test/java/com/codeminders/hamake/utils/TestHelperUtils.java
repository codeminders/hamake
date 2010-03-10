package com.codeminders.hamake.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;


public class TestHelperUtils {
	
	public static String generateTemporaryPath(String folder){			
		Random rand = new Random();
		String tempDir = StringUtils.isEmpty(folder) ? System.getProperty("java.io.tmpdir", "/tmp") : folder ;
		String tempFileName = "hamake-" + Math.abs(rand.nextInt() % 1000);
		return tempDir + File.separator + tempFileName;
	}
	
	public static File generateTemporaryDirectory(String folder){					
		File f = new File(generateTemporaryPath(folder));
		f.mkdirs();
		f.deleteOnExit();
		return f;
	}
	
	public static File generateTemporaryFile(String folder) throws IOException{			
		File f = new File(generateTemporaryPath(folder));
		f.createNewFile();
		f.deleteOnExit();
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
}
