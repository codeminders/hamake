package com.codeminders.hamake;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codeminders.hamake.task.Task;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class CommandThread extends Thread {
	
	public static final Log LOG = LogFactory.getLog(CommandThread.class);

    private Task task;
    private List<HamakePath> inputs;
    private List<HamakePath> outputs;
    private Context exec_context;
    private Semaphore job_semaphore;
    private int rc;

    public CommandThread(Task task,
                         Context exec_context,
                         Semaphore job_semaphore) {
        super(task.toString());
        setDaemon(true);
        this.task = task;
        this.exec_context = exec_context;
        this.job_semaphore = job_semaphore;
    }

    public int getReturnCode() {
        return rc;
    }
    
    public void run() {
        try {
            try {
                rc = task.execute(exec_context);
            } catch (Exception ex) {
            	LOG.error("Execution of command is " + task + " failed", ex);
                rc = -1;
            }
            if (rc != 0)
                try {
                    cleanup();
                } catch (IOException ex) {
                	LOG.error("I/O error during clean up after " + task, ex);
                }
        } finally {
            job_semaphore.release();
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
