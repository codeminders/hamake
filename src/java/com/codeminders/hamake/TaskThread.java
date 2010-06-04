package com.codeminders.hamake;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codeminders.hamake.dtr.DataTransformationRule;

public class TaskThread extends Thread {
	
	public static final Log LOG = LogFactory.getLog(TaskThread.class);

    private Semaphore semaphore;
    private DataTransformationRule task;
    private boolean finished;
    private Condition cv;
    private Lock lock;
    private int rc = 0;

    public TaskThread(DataTransformationRule task, Semaphore semaphore, Lock lock, Condition cv) {
        super("Task " + task.getName());
        setDaemon(true);
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
            rc = task.executeWhenReady(semaphore);
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