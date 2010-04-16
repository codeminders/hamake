package com.codeminders.hamake;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TaskThread extends Thread {
	
	public static final Log LOG = LogFactory.getLog(TaskThread.class);

    private Map<String, Object> context;
    private Semaphore semaphore;
    private Task task;
    private boolean finished;
    private Condition cv;
    private Lock lock;
    private int rc = 0;

    public TaskThread(Task task, Semaphore semaphore, Lock lock, Condition cv, Map<String, Object> context) {
        super("Task " + task.getName());
        setDaemon(true);
        this.context = context;
        this.semaphore = semaphore;
        this.task = task;
        this.cv = cv;
        this.lock = lock;
    }

    public String getTaskName() {
        return task.getName();
    }

    public int getReturnCode() {
        return rc;
    }

    public boolean isFinished() {
        return finished;
    }

    public void run() {
        int rc;
        try {
            rc = task.execute(semaphore, context);
        } catch (Exception ex) {
        	LOG.error("Unexpected exception occured during task " + task.getName(), ex);
            rc = -1000;
        }
        lock.lock();
        try {
            this.rc = rc;
            finished = true;
            cv.signal();
        } finally {
            lock.unlock();
        }
    }
}