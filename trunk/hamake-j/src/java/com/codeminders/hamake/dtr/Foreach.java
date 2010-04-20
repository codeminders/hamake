package com.codeminders.hamake.dtr;

import com.codeminders.hamake.CommandThread;
import com.codeminders.hamake.Config;
import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.Utils;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Data transformation rule that maps a file to one or more files.
 * 
 */
public class Foreach extends DataTransformationRule {
	
	public static final String FULL_FILENAME = "foreach:path";
	public static final String SHORT_FILENAME = "foreach:filename";
	public static final String PARENT_FOLDER = "foreach:folder";
	public static final String FILENAME_WO_EXTENTION = "foreach:basename";
	public static final String EXTENTION = "foreach:ext";

	protected class HamakePathPair {

		public HamakePathPair(HamakePath input, HamakePath output) {
			this.input = input;
			this.outputValue = output;
		}

		public HamakePath input;
		public HamakePath outputValue;
	}

	public static final Log LOG = LogFactory.getLog(Foreach.class);

	private List<HamakePath> inputs;
	private OutputFunction mapFunction;
	private List<HamakePath> deps;
	private Context context;


	public Foreach(Context parentContext, List<HamakePath> inputs, OutputFunction function,
			List<HamakePath> dependencies) {
		this.mapFunction = function;
		this.inputs = inputs;
		this.deps = dependencies;
		this.context = new Context(parentContext);
	}

	@Override
	protected List<HamakePath> getDeps() {
		return deps;
	}

	@Override
	protected List<HamakePath> getInputs() {
		return inputs;
	}

	/**
	 * Return ordered list of output files which corresponds input files
	 * @throws IOException 
	 */
	@Override
	protected List<HamakePath> getOutputs(){
		try{
			return mapFunction.getOutput(inputs.get(0), context);
		}
		catch(IOException e){
			LOG.error(e);
		}
		return null;
	}

	@Override
	public int execute(Semaphore semaphore)
			throws IOException {
		
		List<HamakePath> inputlist;

		for (HamakePath input : inputs) {
			try {
				inputlist = input.getFileList(false);
				if (inputlist == null)
					return -1;
				if (inputlist.isEmpty()) {
					LOG.warn("WARN: Input folder for task " + getName()
							+ " is empty");
					return 0;
				}
			} catch (IOException ex) {
				LOG.error("Error accessing " + input, ex);
				return -1;
			}
		}

		List<HamakePath> outputs = getOutputs();
		if (outputs == null){
			LOG.warn("There were zero files produced by mapping function(s) of " + getName() + " DTR");
			return -1;
		}

		List<HamakePathPair> pathPairs = new ArrayList<HamakePathPair>();

		for (int i = 0; i < inputs.size() && i < outputs.size(); i++) {
			HamakePath input = inputs.get(i);
			HamakePath output = outputs.get(i);

			if (output.getFileStatus().getModificationTime() >= input
					.getFileStatus().getModificationTime()) {
				if (Config.getInstance().verbose)
					LOG.info("Output " + output.getPathName().toString()
							+ " is already present and fresh");
			} else {
				if (Config.getInstance().verbose)
					LOG.info("Output " + output.getPathName().toString()
							+ " is present but not fresh. Removing it.");
				if (!Config.getInstance().dryrun) {
					synchronized (output.getFileSystem()) {
						output.getFileSystem().delete(output.getPathName(),
								true);
						pathPairs.add(new HamakePathPair(input, output));
					}
				}
			}
		}

		if (pathPairs.size() > 0)
			return execQueue(pathPairs, semaphore, context);
		else
			return 0;

	}

	protected int execQueue(List<HamakePathPair> pathPairs,
			Semaphore job_semaphore, Context context) {
		Collection<CommandThread> threads = new ArrayList<CommandThread>();
		for (HamakePathPair pathPair : pathPairs) {
			context.set("$_", pathPair.input.getPathName().toString());
			context.set("$@", pathPair.input.getPathName().getParent().toString());
			context.set("$%", FilenameUtils.getName(pathPair.input.toString()));
			context.set("$#", FilenameUtils.getBaseName(pathPair.input.toString()));
			context.set("$^", FilenameUtils.getExtension(pathPair.input.toString()));
			try{
				List<HamakePath> outputs;
				outputs = mapFunction.getOutput(pathPair.input, context);
				for(HamakePath output : outputs){
					if(!StringUtils.isEmpty(output.getID())) context.set(output.getID(), output);
				}
			}
			catch(IOException e){
				LOG.error(e);
				return -2;
			}
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