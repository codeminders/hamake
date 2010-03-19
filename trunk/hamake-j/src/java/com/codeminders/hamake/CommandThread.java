package com.codeminders.hamake;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class CommandThread extends Thread {

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
                System.err.println("Execution of command is " + command + " failed: " + ex.getMessage());
                if (Config.getInstance().test_mode)
                    ex.printStackTrace();
                rc = -1;
            }
            if (rc != 0)
                try {
                    cleanup();
                } catch (IOException ex) {
                    System.err.println("I/O error during clean up after " + command + ": " + ex.getMessage());
                    if (Config.getInstance().test_mode)
                        ex.printStackTrace();
                }
        } finally {
            job_semaphore.release();
        }
    }

    protected void cleanup() throws IOException {
        FileSystem fs = Utils.getFileSystem(exec_context);
        // TODO this would work only for files, not for paths with masks
        //  use removeIfExists() instead
        for (Path p : cleanuplist) {
            boolean exists;
            synchronized (fs) {
                exists = fs.exists(p);
                if (exists)
                    if (Config.getInstance().verbose)
                        System.err.println("Removing " + p.toUri());
                if (!Config.getInstance().dryrun) {
                    synchronized (fs) {
                        fs.delete(p, true);
                    }
                }
            }
        }
    }
}
