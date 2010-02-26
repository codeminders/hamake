package com.codeminders.hamake;

import org.apache.hadoop.hdfs.DFSClient;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.io.IOException;

public class CommandThread extends Thread {

    private Command command;
    private Map<String, Collection> params;
    private Collection<String> cleanuplist;
    private Map<String, Object> exec_context;
    private Semaphore job_semaphore;
    private int rc;

    public CommandThread(Command command,
                         Map<String, Collection> params,
                         Collection<String> cleanuplist,
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
        DFSClient fsclient = Utils.getFSClient(exec_context);
        // TODO this would work only for files, not for paths with masks
        //  use removeIfExists() instead
        for (String c : cleanuplist) {
            boolean exists;
            synchronized (fsclient) {
                exists = fsclient.exists(c);
                if (exists)
                    if (Config.getInstance().verbose)
                        System.err.println("Removing " + c);
                if (!Config.getInstance().dryrun) {
                    synchronized (fsclient) {
                        fsclient.delete(c, true);
                    }
                }
            }
        }
    }
}
