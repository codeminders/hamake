package com.codeminders.hamake.dtr;

import com.codeminders.hamake.CommandThread;
import com.codeminders.hamake.InvalidContextStateException;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.data.DataFunction;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data transformation rule that maps a file to one or more files.
 * 
 */
public class Foreach extends DataTransformationRule {
	
	private class ExecQueueItem{
		private CommandThread command;
		private Thread thread;
		private Path inputPath;
		private long startTimeStamp;
		
		public ExecQueueItem(CommandThread command, Thread thread, Path inputPath, Long startTimeStamp){
			this.thread = thread;
			this.command = command;
			this.inputPath = inputPath;
			this.startTimeStamp = startTimeStamp;
		}

		public CommandThread getCommand() {
			return command;
		}

		public Thread getThread() {
			return thread;
		}

		public Path getInputPath() {
			return inputPath;
		}

		public long getStartTimeStamp() {
			return startTimeStamp;
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
	private boolean deleteFirst = true;


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
	
	public boolean getDeleteFirst() {
		return deleteFirst;
	}

	public void setDeleteFirst(boolean deleteFirst) {
		this.deleteFirst = deleteFirst;
	}
	
	@Override
	public int execute(Semaphore semaphore)
			throws IOException {
		
		LOG.info(getName() + ": Listing input data");
		List<Path> inputlist = input.getPath(getContext());
		if (inputlist == null || inputlist.isEmpty()){
			LOG.error("Input folder of DTR " + getName()
					+ " is empty");
			return -1;
		}
		LOG.info(getName() + ": Has " + inputlist.size() + " files to process");
		QueueFetcher fetcher = new QueueFetcher();
		fetcher.start();
		for(Path ipath : inputlist){
			CommandThread command = new CommandThread(getTask(), getContext(), semaphore);
			FileSystem inputfs = ipath.getFileSystem((Configuration)getContext().get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
			command.getContext().setForbidden(FULL_FILENAME_VAR, ipath.toString());
			command.getContext().setForbidden(SHORT_FILENAME_VAR, FilenameUtils.getName(ipath.toString()));
			command.getContext().setForbidden(PARENT_FOLDER_VAR, ipath.getParent().toString());
			command.getContext().setForbidden(FILENAME_WO_EXTENTION_VAR, FilenameUtils.getBaseName(ipath.toString()));
			command.getContext().setForbidden(EXTENTION_VAR, FilenameUtils.getExtension(ipath.toString()));
			long inputTimeStamp = Utils.recursiveGetModificationTime(inputfs, ipath);
			boolean inTrash = false;
			if(getTrashBucket() != null){
				for(Path trashBucketPath : getTrashBucket().getPath(command.getContext())){
					FileSystem trashBucketFS = trashBucketPath.getFileSystem((Configuration)command.getContext().get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
					FileStatus[] statuses = trashBucketFS.listStatus(trashBucketPath);
					if(statuses != null){
						for(FileStatus status : statuses){
							if(status.getPath().getName().equals(ipath.getName())){
								if(Utils.recursiveGetModificationTime(trashBucketFS, trashBucketPath) >= inputTimeStamp){
									LOG.info(getName() + ": Passing " + ipath.toString() + ". This file is in the trash bucket " + trashBucketPath.toString());
									inTrash = true;
									break;
								}
							}
						}
					}
				}
			}
			if(!inTrash){
				for (DataFunction outputFunc : output) {
					long outputTimeStamp = outputFunc.getMaxTimeStamp(command.getContext());
					outputTimeStamp = (outputTimeStamp == 0)? -1 : outputTimeStamp;
					if (outputTimeStamp < inputTimeStamp || outputTimeStamp == -1) {
						if (deleteFirst)
							outputFunc.clear(command.getContext());
						try {
							semaphore.acquire();
						} catch (InterruptedException e) {
							LOG.error(getName() + ": Error running " + getName(), e);
							break;
						}
						ExecQueueItem item = new ExecQueueItem(command, new Thread(command, getTask().toString()), ipath, System.currentTimeMillis());
						item.getThread().setDaemon(true);
						item.getThread().start();
						if(fetcher.isAlive()) fetcher.pushQueue(item);
					} 
				}
			}
		}
		fetcher.terminate();
		try {
			fetcher.join();
		} catch (InterruptedException e) {
			LOG.info(getName() + ": Error running " + getName(), e);
		}
		LOG.info(getName() + ": Processed " + fetcher.getCounter() + " files, " + fetcher.getErrors() + " files with errors, run time: " + fetcher.getTotalRunTime() / inputlist.size() + " ms");
		return fetcher.getResult();		
	}
	
	protected class QueueFetcher extends Thread{
		
		private int result = 0;
		private long counter = 0;
		private long errors = 0;
		private AtomicBoolean terminated = new AtomicBoolean(false);
		private Queue<ExecQueueItem> queue = new ConcurrentLinkedQueue<ExecQueueItem>();
		private AtomicLong totalRunTime = new AtomicLong(0);
		
		public void pushQueue(ExecQueueItem queue) {
			this.queue.add(queue);
		}

		public int getResult() {
			return result;
		}
		
		public long getCounter() {
			return counter;
		}

		public long getErrors() {
			return errors;
		}

		public long getTotalRunTime() {
			return totalRunTime.get();
		}

		public void terminate(){
			terminated.set(true);
		}

		@Override
		public void run(){
			while(!terminated.get() || !queue.isEmpty()){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					LOG.error("Error in QueueFetcher", e);
					result = -1000;
					return;
				}
				ExecQueueItem item = null;
				while ((item = queue.poll()) != null) {
					try {
						item.getThread().join();
						totalRunTime.getAndAdd(System.currentTimeMillis() - item.getStartTimeStamp());
					} catch (InterruptedException ex) {
						LOG.error("Error in QueueFetcher", ex);
						result = -1000;
						return;
					}
					int t_rc = item.getCommand().getReturnCode();
					if (t_rc != 0){
						errors++;
						result = t_rc;
						try {
							throwInTrashBucket(item.getInputPath(), item.getCommand().getContext(), isCopyIncorrectFile());
						} catch (IOException e) {
							LOG.error("Error moving file to trash", e);
						}
					}
					else counter++;
				}
			}
		}
	}

	protected void throwInTrashBucket(Path file, Context context, boolean copySource) throws IOException{
		if(getTrashBucket() != null){
			FileSystem fileFs = file.getFileSystem((Configuration)context.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
			for(Path trashBucketPath : getTrashBucket().getPath(context)){
				FileSystem trashBucketFS = trashBucketPath.getFileSystem((Configuration)context.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
				if(!trashBucketFS.exists(trashBucketPath)){
					trashBucketFS.mkdirs(trashBucketPath);
				}
				if(!trashBucketFS.getFileStatus(trashBucketPath).isDir()){
					throw new IOException("Could not put file " + file.getName() + " to trash " + trashBucketPath.toString() + " because is is not a folder");
				}
				Path dstFile = new Path(trashBucketPath, file.getName());
				if(trashBucketFS.exists(dstFile))trashBucketFS.delete(dstFile, false);
				if(copySource) FileUtil.copy(fileFs, file, trashBucketFS, trashBucketPath, false, (Configuration)context.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
				else{
					FSDataOutputStream fileStream = trashBucketFS.create(dstFile);
					if(fileStream != null) fileStream.close();
				}
			}
		}
	}

}