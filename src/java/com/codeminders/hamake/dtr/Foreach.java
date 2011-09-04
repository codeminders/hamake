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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
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
		private List<Path> inputPaths;
		private long startTimeStamp;
		
		public ExecQueueItem(CommandThread command, Thread thread, List<Path> inputPaths, Long startTimeStamp){
			this.thread = thread;
			this.command = command;
			this.inputPaths = inputPaths;
			this.startTimeStamp = startTimeStamp;
		}

		public CommandThread getCommand() {
			return command;
		}

		public Thread getThread() {
			return thread;
		}

		public List<Path> getInputPaths() {
			return inputPaths;
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
	private int batchSize = 1;
	private boolean deleteFirst = true;
	private int parallelism = -1;


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
	
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
	
	@Override
	public Context getGenericOutputContext()
	{
		Context context = new Context( getContext());
		context.setForbidden(FULL_FILENAME_VAR, new String[] { ""});
		context.setForbidden(SHORT_FILENAME_VAR, new String[] { ""});
		context.setForbidden(PARENT_FOLDER_VAR, new String[] { ""});
		context.setForbidden(FILENAME_WO_EXTENTION_VAR, new String[] { ""});
		context.setForbidden(EXTENTION_VAR, new String[] { ""});
		return context;
	}

	@Override
	public int execute(Semaphore semaphore)
			throws IOException {
		
		LOG.info(getName() + ": Listing input data");
		List<Path> inputlist = input.getPath(getContext());
		if (inputlist == null || inputlist.isEmpty()){
			LOG.warn("Input folder of DTR " + getName()
					+ " is empty");
			return 0;
		}
		LOG.info(getName() + ": Has " + inputlist.size() + " files to process");

		Map<List<String>, List<Path>> inputPathsByOutputList = new HashMap<List<String>, List<Path>>();
		int inputNumber = 0;
		for(Path ipath : inputlist){
			++inputNumber;
			Context icontext = new Context(getContext());
			icontext.setForbidden(FULL_FILENAME_VAR, ipath.toString());
			icontext.setForbidden(SHORT_FILENAME_VAR, FilenameUtils.getName(ipath.toString()));
			icontext.setForbidden(PARENT_FOLDER_VAR, ipath.getParent().toString());
			icontext.setForbidden(FILENAME_WO_EXTENTION_VAR, FilenameUtils.getBaseName(ipath.toString()));
			icontext.setForbidden(EXTENTION_VAR, FilenameUtils.getExtension(ipath.toString()));
			FileSystem inputfs = ipath.getFileSystem((Configuration)getContext().get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
			long inputTimeStamp = Utils.recursiveGetModificationTime(inputfs, ipath);
			boolean inTrash = false;
			if(getTrashBucket() != null){
				for(Path trashBucketPath : getTrashBucket().getPath(icontext)){
					FileSystem trashBucketFS = trashBucketPath.getFileSystem((Configuration)icontext.get(Context.HAMAKE_PROPERTY_HADOOP_CONFIGURATION));
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
					long outputTimeStamp = outputFunc.getMaxTimeStamp(icontext);
					outputTimeStamp = (outputTimeStamp == 0)? -1 : outputTimeStamp;
					if (outputTimeStamp < inputTimeStamp || outputTimeStamp == -1) {
						if (deleteFirst)
							outputFunc.clear(icontext);
						List<Path> outputPathList = outputFunc.getPath(icontext);
						List<String> outputFullFilenameList = new ArrayList<String>(outputPathList.size());
						for (Path outputPath : outputPathList)
							outputFullFilenameList.add(outputPath.toString());
						List<Path> inputPaths = inputPathsByOutputList.get(outputFullFilenameList);
						if (inputPaths == null)
						{
							inputPaths = new ArrayList<Path>();
							inputPathsByOutputList.put(outputFullFilenameList, inputPaths);
						}
						inputPaths.add(ipath);
					}
				}
			}
		}

		QueueFetcher fetcher = new QueueFetcher();
		fetcher.start();

		if (!inputPathsByOutputList.isEmpty())
		{
			Semaphore taskSemaphore;
			if (parallelism <= 0)
				taskSemaphore = new Semaphore(0) {
					@Override
					public void acquire() throws InterruptedException {
						// DO NOTHING
					}

					public void release() {
						// DO NOTHING
					}

				};
			else
				taskSemaphore = new Semaphore(parallelism);

			int outputSetCount = 0;
			int effectiveBatchSize = batchSize == 0 ? inputPathsByOutputList.size() : batchSize;
			List<Path> inputPaths = new ArrayList<Path>();
			List<String> fullFilenames = new ArrayList<String>();
			List<String> shortFilenames = new ArrayList<String>();
			List<String> parentFolders = new ArrayList<String>();
			List<String> basenames = new ArrayList<String>();
			List<String> extensions = new ArrayList<String>();
			List<Map.Entry<List<String>, List<Path>>> entryList = new ArrayList<Map.Entry<List<String>, List<Path>>>(inputPathsByOutputList.entrySet());
			Collections.sort(entryList, new Comparator<Map.Entry<List<String>, List<Path>>>()
				{
					public int compare(Map.Entry<List<String>, List<Path>> a, Map.Entry<List<String>, List<Path>> b)
					{
						List<String> keyA = a.getKey();
						List<String> keyB = b.getKey();
						int common = Math.min(keyA.size(), keyB.size());
						for (int i = 0; i < common; ++i)
						{
							int x = keyA.get(i).compareTo(keyB.get(i));
							if (x != 0)
								return x;
						}
						return keyA.size() - keyB.size();
					}
				});
			for (Map.Entry<List<String>, List<Path>> entry : entryList)
			{
				for (Path ipath : entry.getValue())
				{
					inputPaths.add(ipath);
					fullFilenames.add(ipath.toString());
					shortFilenames.add(FilenameUtils.getName(ipath.toString()));
					parentFolders.add(ipath.getParent().toString());
					basenames.add(FilenameUtils.getBaseName(ipath.toString()));
					extensions.add(FilenameUtils.getExtension(ipath.toString()));
				}

				++outputSetCount;

				if (outputSetCount == inputPathsByOutputList.size() || (batchSize > 0 && (outputSetCount % batchSize) == 0))
				{
					CommandThread command = new CommandThread(getTask(), getContext(), semaphore, taskSemaphore);
					command.getContext().setForbidden(FULL_FILENAME_VAR, fullFilenames.toArray(new String[0]));
					command.getContext().setForbidden(SHORT_FILENAME_VAR, shortFilenames.toArray(new String[0]));
					command.getContext().setForbidden(PARENT_FOLDER_VAR, parentFolders.toArray(new String[0]));
					command.getContext().setForbidden(FILENAME_WO_EXTENTION_VAR, basenames.toArray(new String[0]));
					command.getContext().setForbidden(EXTENTION_VAR, extensions.toArray(new String[0]));

					try {
						taskSemaphore.acquire();
					} catch (InterruptedException e) {
						LOG.error(getName() + ": Error running " + getName(), e);
						break;
					}
					try {
						semaphore.acquire();
					} catch (InterruptedException e) {
						LOG.error(getName() + ": Error running " + getName(), e);
						break;
					}
					ExecQueueItem item = new ExecQueueItem(command, new Thread(command, getTask().toString()), new ArrayList<Path>(inputPaths), System.currentTimeMillis());
					item.getThread().setDaemon(true);
					item.getThread().start();
					if(fetcher.isAlive()) fetcher.pushQueue(item);

					inputPaths.clear();
					fullFilenames.clear();
					shortFilenames.clear();
					parentFolders.clear();
					basenames.clear();
					extensions.clear();
				}
			}
		}

		fetcher.terminate();
		try {
			fetcher.join();
		} catch (InterruptedException e) {
			LOG.info(getName() + ": Error running " + getName(), e);
		}
		LOG.info(getName() + ": Completed " + fetcher.getCounter() + " tasks, " + fetcher.getErrors() + " tasks with errors, average run time: " + fetcher.getTotalRunTime() / (fetcher.getCounter() + fetcher.getErrors()) + " ms");
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
						for (Path ipath : item.getInputPaths())
						{
							try {
								throwInTrashBucket(ipath, item.getCommand().getContext(), isCopyIncorrectFile());
							} catch (IOException e) {
								LOG.error("Error moving file to trash", e);
							}
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