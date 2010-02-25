package com.codeminders.hamake;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TaskRunner {

    /*

    def run(self):
        while True:
            self.cv.acquire()
            running_names = [x.name for x in self.running]
            candidates = [x for x in self.graph.getReadyToRunTasks() if x not in self.failed and x not in running_names]
            if len(candidates)==0 and len(self.running)==0:
                break

            self.startTasks(candidates)

            self.cv.wait()
            just_finished = [x for x in self.running if x.finished]
            for f in just_finished:
                if f.rc == 0:
                    print >> sys.stderr, "Execution of %s is completed" % (f.name)
                    self.completed.append(f.name)
                    self.graph.removeTask(f.name)
                else:
                    print >> sys.stderr, "Execution of %s is failed with code %d" % (f.name, f.rc)
                    self.failed.append(f.name)
            self.running = [x for x in self.running if not x.finished]
            self.cv.release()

     */

    private ExecutionGraph graph;
    private Map<String, Object> context;
    private Map<String, Task> tasks;

    private Collection<String> completed;
    private Collection<String> failed;
    private Collection<TaskThread> running;

    private Semaphore job_semaphore;
    private Lock lock;
    private Condition condition;

    public TaskRunner(Collection<Task> tasks,
                      int numJobs,
                      Collection<String> targets,
                      Map<String, Object> context) {
        if (Config.getInstance().nodeps)
            graph = new NoDepsExecutionGraph(tasks, targets);
        else
            graph = new DependencyExecutionGraph(tasks, targets);
        this.context = context;
        this.tasks = new HashMap<String, Task>();
        for (Task t : tasks)
            this.tasks.put(t.getName(), t);
        this.completed = new HashSet<String>();
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
        while (true) {
            lock.lock();

            Set<String> runningNames = new HashSet<String>();
            for (TaskThread tt : running) {
                runningNames.add(tt.getTaskName());
            }
            Collection<String> candidates = new ArrayList<String>();
            for (String task : graph.getReadyForRunTasks()) {
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
                // TODO
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
                    completed.add(tt.getTaskName());
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
}
