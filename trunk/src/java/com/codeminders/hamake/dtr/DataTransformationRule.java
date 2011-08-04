package com.codeminders.hamake.dtr;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.context.ContextAware;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.task.Task;

public abstract class DataTransformationRule extends ContextAware{

    public static final Log LOG = LogFactory.getLog(DataTransformationRule.class);

	private String name;
	private Task task;
	private DataFunction trashBucket;
	private boolean copyIncorrectFile = false;
	
	public DataTransformationRule(Context parentContext){
		super(parentContext);
	}
	
	protected abstract List<? extends DataFunction> getInputs();
	
	protected abstract List<? extends DataFunction> getOutputs();
	
	protected abstract List<? extends DataFunction> getDeps();
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public Context getGenericOutputContext()
	{
		return getContext();
	}

	public boolean dependsOn(DataTransformationRule t) throws IOException {		
		Context context = t.getGenericOutputContext();
		for (DataFunction i : getInputs()) {
			for (DataFunction o : t.getOutputs()) {
				if (i.intersects(context, o))
					return true;
			}
		}
		if(getDeps() != null){
			for (DataFunction i : getDeps()) {
				for (DataFunction o : t.getOutputs()) {
					if (i.intersects(context, o)) return true;
				}
			} 
		}
		return false;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}
	
	public boolean isCopyIncorrectFile() {
		return copyIncorrectFile;
	}

	public void setCopyIncorrectFile(boolean removeIncorrectFile) {
		this.copyIncorrectFile = removeIncorrectFile;
	}

	public DataFunction getTrashBucket() {
		return trashBucket;
	}

	public void setTrashBucket(DataFunction trashBucket) {
		this.trashBucket = trashBucket;
	}

	@Override
	public String toString() {
		return getName();
	}
	
	/**
	 * Start DTR as soon as all dependencies are satisfied
	 * @param semaphore
	 * @return
	 * @throws IOException
	 */
	public int executeWhenReady(Semaphore semaphore) throws IOException{
		if((Boolean)getContext().get(Context.HAMAKE_PROPERTY_WITH_DEPENDENCIES)){
			for(DataFunction depDataFunc : getDeps()){
				for(Path path : depDataFunc.getPath(getContext())){
					FileSystem fs = depDataFunc.getFileSystem(getContext(), path);
					if(!fs.exists(path)){
						LOG.warn("DTR " + getName() + " is waiting for " + path);
					}
					while(!fs.exists(path)){
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOG.error(e);
						}
					}
				}
			}
		}
		return execute(semaphore);
	}

	protected abstract int execute(Semaphore semaphore)
			throws IOException;		
	
}
