package com.codeminders.hamake.dtr;

import com.codeminders.hamake.CommandThread;
import com.codeminders.hamake.InvalidContextStateException;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.DataFunction;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Data transformation rule that maps a file to one or more files.
 * 
 */
public class Foreach extends DataTransformationRule {
	
	private class ExecQueueItem{
		private CommandThread command;
		private Thread thread;
		
		public ExecQueueItem(CommandThread command, Thread thread){
			this.thread = thread;
			this.command = command;
		}

		public CommandThread getCommand() {
			return command;
		}

		public Thread getThread() {
			return thread;
		}
		
	}
	
	public static final String FULL_FILENAME_VAR = Context.FOREACH_VARS_PREFIX + "path";
	public static final String SHORT_FILENAME_VAR = Context.FOREACH_VARS_PREFIX + "filename";
	public static final String PARENT_FOLDER_VAR = Context.FOREACH_VARS_PREFIX + "folder";
	public static final String FILENAME_WO_EXTENTION_VAR = Context.FOREACH_VARS_PREFIX + "basename";
	public static final String EXTENTION_VAR = Context.FOREACH_VARS_PREFIX + "ext";

	public static final Log LOG = LogFactory.getLog(Foreach.class);

	private DataFunction input;
	private List<? extends DataFunction> output;
	private List<? extends DataFunction> deps;


	public Foreach(Context parentContext, DataFunction input, List<? extends DataFunction> output,
			List<? extends DataFunction> dependencies) throws InvalidContextStateException {
		super(parentContext);
		this.output = output;
		this.input = input;
		this.deps = dependencies;
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
	public int execute(Semaphore semaphore)
			throws IOException {
		
		List<Path> inputlist = input.getPath(getContext());
		List<ExecQueueItem> queue = new ArrayList<ExecQueueItem>();
		if (inputlist == null || inputlist.isEmpty()){
			LOG.error("Input folder of DTR " + getName()
					+ " is empty");
			return -1;
		}
		
		for(Path ipath : inputlist){
			CommandThread command = new CommandThread(getTask(), getContext(), semaphore);
			FileSystem inputfs = input.getFileSystem(getContext(), ipath);
			if(!inputfs.exists(ipath)){
				LOG.error(ipath.toString() + " from input of DTR " + getName()
						+ " does not exist. Ignoring");
				continue;
			}
			command.getContext().setForbidden(FULL_FILENAME_VAR, ipath.toString());
			command.getContext().setForbidden(SHORT_FILENAME_VAR, FilenameUtils.getName(ipath.toString()));
			command.getContext().setForbidden(PARENT_FOLDER_VAR, ipath.getParent().toString());
			command.getContext().setForbidden(FILENAME_WO_EXTENTION_VAR, FilenameUtils.getBaseName(ipath.toString()));
			command.getContext().setForbidden(EXTENTION_VAR, FilenameUtils.getExtension(ipath.toString()));
			long inputTimeStamp = inputfs.getFileStatus(ipath).getModificationTime();
			for (DataFunction outputFunc : output) {
				if (outputFunc.getMaxTimeStamp(command.getContext()) < inputTimeStamp) {
					LOG.info("adding " + ipath.toString());
					outputFunc.clear(command.getContext());
					queue.add(new ExecQueueItem(command, new Thread(command, getTask().toString())));
				} 
			}
		}
		if(queue.size() > 0) return execQueue(queue, semaphore);		
		else{
			LOG.info("all output data of DTR "+ getName() + " is fresh enough");
			return 0;
		}
	}

	protected int execQueue(List<ExecQueueItem> queue, Semaphore jobSemaphore) {
		for (ExecQueueItem item : queue) {
			try {
				jobSemaphore.acquire();
			} catch (InterruptedException ex) {
				LOG.error(ex);
				return -1000;
			}
			try {
				item.getThread().setDaemon(true);
				item.getThread().start();
			} catch (Exception ex) {
				LOG.error(ex);
				jobSemaphore.release();
			}
		}
		int rc = 0;
		for (ExecQueueItem item : queue) {
			try {
				item.getThread().join();
			} catch (InterruptedException ex) {
				LOG.error(ex);
				return -1000;
			}
			int t_rc = item.getCommand().getReturnCode();
			if (t_rc != 0)
				rc = t_rc;
		}
		return rc;

	}

}