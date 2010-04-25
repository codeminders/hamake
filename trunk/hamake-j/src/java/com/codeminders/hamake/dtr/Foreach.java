package com.codeminders.hamake.dtr;

import com.codeminders.hamake.CommandThread;
import com.codeminders.hamake.Config;
import com.codeminders.hamake.Context;
import com.codeminders.hamake.data.DataFunction;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Data transformation rule that maps a file to one or more files.
 * 
 */
public class Foreach extends DataTransformationRule {
	
	public static final String FULL_FILENAME_VAR = "path";
	public static final String SHORT_FILENAME_VAR = "filename";
	public static final String PARENT_FOLDER_VAR = "folder";
	public static final String FILENAME_WO_EXTENTION_VAR = "basename";
	public static final String EXTENTION_VAR = "ext";

	public static final Log LOG = LogFactory.getLog(Foreach.class);

	private DataFunction input;
	private List<? extends DataFunction> output;
	private List<? extends DataFunction> deps;
	private Context context;


	public Foreach(Context parentContext, DataFunction input, List<? extends DataFunction> output,
			List<? extends DataFunction> dependencies) {
		this.output = output;
		this.input = input;
		this.deps = dependencies;
		this.context = new Context(parentContext);
	}

	@Override
	protected List<? extends DataFunction> getDeps() {
		return deps;
	}

	@Override
	protected List<? extends DataFunction> getInputs() {
		return Arrays.asList(input);
	}

	@Override
	protected List<? extends DataFunction> getOutputs(){
		return output;
	}
	
	@Override
	protected Context getContext() {
		return context;
	}

	@Override
	public int execute(Semaphore semaphore)
			throws IOException {
		
		List<Path> inputlist = input.getPath(context);
		List<Context> pathPairs = new ArrayList<Context>();
		if (inputlist == null || inputlist.isEmpty()){
			LOG.error("Input folder of DTR " + getName()
					+ " is empty");
			return -1;
		}
		
		for(Path path : inputlist){
			FileSystem inputfs = input.getFileSystem(context, path);
			if(inputfs.getFileStatus(path).isDir()){
				LOG.error(path.toString() + " from input of DTR " + getName()
						+ " is not a file. Ignoring");
				continue;
			}
			Context context = new Context(this.context);
			context.setForeach(FULL_FILENAME_VAR, path.toString());
			context.setForeach(SHORT_FILENAME_VAR, FilenameUtils.getName(path.toString()));
			context.setForeach(PARENT_FOLDER_VAR, path.getParent().toString());
			context.setForeach(FILENAME_WO_EXTENTION_VAR, FilenameUtils.getBaseName(path.toString()));
			context.setForeach(EXTENTION_VAR, FilenameUtils.getExtension(path.toString()));
			for (DataFunction outputFunc : output) {
				if(outputFunc.isFolder(context)){
					LOG.error("Output of DTR " + getName()
							+ " contains not only files");
					return 1;
				}
				if (outputFunc.getMinTimeStamp(context) >= input.getMinTimeStamp(context)) {
					if (Config.getInstance().verbose){
						LOG.info("Output " + outputFunc.getPath(context)
								+ " is already present and fresh");
						return 0;
					}
				} 
				else{
					outputFunc.clear(context);
				}
			}
			pathPairs.add(context);
		}
		if(pathPairs.size() > 0) return execQueue(pathPairs, semaphore);
		else return 0;
	}

	protected int execQueue(List<Context> contexes,
			Semaphore job_semaphore) {
		Collection<CommandThread> threads = new ArrayList<CommandThread>();
		for (Context context : contexes) {
			try {
				job_semaphore.acquire();
			} catch (InterruptedException ex) {
				LOG.error(ex);
				return -1000;
			}
			try {
				CommandThread t = new CommandThread(getTask(), context,
						job_semaphore);
				threads.add(t);
				t.start();
			} catch (Exception ex) {
				LOG.error(ex);
				job_semaphore.release();
			}
		}
		int rc = 0;
		for (CommandThread t : threads) {
			try {
				t.join();
			} catch (InterruptedException ex) {
				LOG.error(ex);
				return -1000;
			}
			int t_rc = t.getReturnCode();
			if (t_rc != 0)
				rc = t_rc;
		}
		return rc;

	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("inputs", getInputs()).append(
				"deps", getDeps()).appendSuper(super.toString()).toString();
	}

}