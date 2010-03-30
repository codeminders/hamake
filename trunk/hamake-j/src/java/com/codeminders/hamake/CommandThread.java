package com.codeminders.hamake;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class CommandThread extends Thread {
	
	public static final Log LOG = LogFactory.getLog(CommandThread.class);

    private Command command;
    private Map<String, Collection> params;
    private Collection<Path> cleanuplist;
    private Map<String, Object> exec_context;
    private Semaphore job_semaphore;
    private int rc;

    public CommandThread(Command command,
                         Map<String, Collection> params,
                         Collection<Path> cleanuplist,
                         Map<String, Object> exec_context,
                         Semaphore job_semaphore) {
        super(command.toString());
        setDaemon(true);
        this.command = command;
        this.params = params;
        this.cleanuplist = cleanuplist;
        this.exec_context = exec_context;
        this.job_semaphore = job_semaphore;
    }

    public int getReturnCode() {
        return rc;
    }
    
    public void run() {
        try {
            try {
                rc = command.execute(params, exec_context);
            } catch (Exception ex) {
            	LOG.error("Execution of command is " + command + " failed", ex);
                rc = -1;
            }
            if (rc != 0)
                try {
                    cleanup();
                } catch (IOException ex) {
                	LOG.error("I/O error during clean up after " + command, ex);
                }
        } finally {
            job_semaphore.release();
        }
    }

    protected void cleanup() throws IOException {        
        // TODO this would work only for files, not for paths with masks
        //  use removeIfExists() instead
    	Configuration conf = (Configuration)exec_context.get("hamake.configuration");
        for (Path p : cleanuplist) {
            boolean exists;
            synchronized (exec_context) {
            	FileSystem fs = p.getFileSystem(conf);
                exists = fs.exists(p);
                if (exists)
                    if (Config.getInstance().verbose)
                    	LOG.info("Removing " + p.toUri());
                if (!Config.getInstance().dryrun) {
                    synchronized (fs) {
                        fs.delete(p, true);
                    }
                }
            }
        }
    }
}
