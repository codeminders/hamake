package com.codeminders.hamake;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class TaskThread extends Thread {

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
            System.err.println("Unexpected exception occured during task " + task.getName() + " execution");
            if (Config.getInstance().test_mode)
                ex.printStackTrace();
            rc = -1000;
        }
        lock.lock();
        try {
            this.rc = rc;
            finished = true;
            cv.notify();
        } finally {
            lock.unlock();
        }
    }
}