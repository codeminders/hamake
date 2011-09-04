package com.codeminders.hamake;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.context.ContextAware;
import com.codeminders.hamake.task.Task;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public class CommandThread extends ContextAware implements Runnable{
	
	public static final Log LOG = LogFactory.getLog(CommandThread.class);

    private Task task;
    private Semaphore jobSemaphore;
    private Semaphore taskSemaphore;
    private int rc = -200;

    public CommandThread(Task task,
                         Context parentContext,
                         Semaphore jobSemaphore,
                         Semaphore taskSemaphore) {
    	super(parentContext);
        this.task = task;
        this.jobSemaphore = jobSemaphore;
        this.taskSemaphore = taskSemaphore;
    }

    public int getReturnCode() {
        return rc;
    }
    
    public void run() {
        try {
            try {
                rc = task.execute(getContext());
            } catch (Exception ex) {
            	LOG.error("Execution of command " + task + " failed", ex);
                rc = -1;
            }
            if (rc != 0)
                try {
                    cleanup();
                } catch (IOException ex) {
                	LOG.error("I/O error during clean up after " + task, ex);
                }
        } finally {
        	jobSemaphore.release();
        	taskSemaphore.release();
        }
    }

    protected void cleanup() throws IOException {        
        // TODO this would work only for files, not for paths with masks
        //  use removeIfExists() instead
//    	Configuration conf = (Configuration)exec_context.get("hamake.configuration");
//        for (Path p : cleanuplist) {
//            boolean exists;
//            synchronized (exec_context) {
//            	FileSystem fs = p.getFileSystem(conf);
//                exists = fs.exists(p);
//                if (exists)
//                    if (Config.getInstance().verbose)
//                    	LOG.info("Removing " + p.toUri());
//                if (!Config.getInstance().dryrun) {
//                    synchronized (fs) {
//                        fs.delete(p, true);
//                    }
//                }
//            }
//        }
    }
}
