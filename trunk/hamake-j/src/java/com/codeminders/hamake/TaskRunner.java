package com.codeminders.hamake;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TaskRunner {

    private ExecutionGraph graph;
    private Map<String, Object> context;
    private Map<String, Task> tasks;
    private String startTask;

    private Collection<String> failed;
    private Collection<TaskThread> running;

    private Semaphore job_semaphore;
    private Lock lock;
    private Condition condition;

    public TaskRunner(List<Task> tasks,
                      int numJobs,
                      Collection<String> targets,
                      Map<String, Object> context, String startTask) {
        if (Config.getInstance().nodeps)
            graph = new NoDepsExecutionGraph(tasks);
        else
            graph = new DependencyExecutionGraph(tasks);
        this.context = context;
        this.tasks = new HashMap<String, Task>();
        for (Task t : tasks)
            this.tasks.put(t.getName(), t);
        this.failed = new HashSet<String>();
        this.running = new HashSet<TaskThread>();
        if (numJobs < 0)
            job_semaphore = new Semaphore(0) {
                @Override
                public void acquire()
                        throws InterruptedException {
                    // DO NOTHING
                }

                public void release() {
                    // DO NOTHING
                }

            };
        else
            job_semaphore = new Semaphore(numJobs);
        lock = new ReentrantLock();
        condition = lock.newCondition();
        this.startTask = startTask; 
    }

    public void startTasks(Collection<String> tasks) {
        for (String task : tasks) {
            System.err.println("Starting " + task);
            Task t = this.tasks.get(task);
            TaskThread tt = new TaskThread(t, job_semaphore, lock, condition, context);
            running.add(tt);
            tt.start();
        }
    }

    int getFailed() {
        return failed.size();
    }

    void run() {
        Thread shutdownHook = new Thread() {
            @Override
            public void run() {
                System.out.println("Program was terminated unexpectedly. Make sure HaMake task dosn't call System.exit()");
            }
        };

        Runtime.getRuntime().addShutdownHook( shutdownHook );
        try
        {
            while (true) {
                lock.lock();

                Set<String> runningNames = new HashSet<String>();
                for (TaskThread tt : running) {
                    runningNames.add(tt.getTaskName());
                }
                Collection<String> candidates = new ArrayList<String>();                
                for (String task : graph.getReadyForRunTasks(startTask)) {
                    if (!runningNames.contains(task) &&
                            !failed.contains(task)) {
                        candidates.add(task);
                    }
                }
                if (candidates.isEmpty() && running.isEmpty()) {
                    break;
                }

                startTasks(candidates);

                try {
                    condition.await();
                } catch (InterruptedException ex) {
                    System.err.println("Execution is interrupted");
                    return;
                }

                Collection<TaskThread> justFinished = new ArrayList<TaskThread>();
                Collection<TaskThread> stillRunning = new ArrayList<TaskThread>();

                for (TaskThread t : running) {
                    if (t.isFinished()) {
                        justFinished.add(t);
                    } else {
                        stillRunning.add(t);
                    }
                }
                for (TaskThread tt : justFinished) {
                    if (tt.getReturnCode() == 0) {
                        System.err.println("Execution of " + tt.getTaskName() + " is completed");
                        graph.removeTask(tt.getTaskName());
                    } else {
                        System.err.println("Execution of " + tt.getTaskName() + " is failed with code " + tt.getReturnCode());
                        failed.add(tt.getTaskName());
                        graph.removeTask(tt.getTaskName());

                    }
                }
                running = stillRunning;
                lock.unlock();
            }
        }
        finally {
            Runtime.getRuntime().removeShutdownHook( shutdownHook );
        }

    }
}
