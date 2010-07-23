package com.codeminders.hamake;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codeminders.hamake.dtr.DataTransformationRule;

public class TaskRunner {

	public static final Log LOG = LogFactory.getLog(TaskRunner.class);

	private ExecutionGraph graph;
	private Map<String, DataTransformationRule> tasks;
	private List<String> targets;

	private Set<String> failed;
	private Set<TaskThread> running;

	private Semaphore job_semaphore;
	private Lock lock;
	private Condition condition;

	@SuppressWarnings("serial")
    public TaskRunner(List<DataTransformationRule> tasks, int numJobs, List<String> targets) throws IOException {
		LOG.info("Calculating dependencies");
		graph = new NoDepsExecutionGraph(tasks);
		this.tasks = new HashMap<String, DataTransformationRule>();
		for (DataTransformationRule t : tasks)
			this.tasks.put(t.getName(), t);
		this.failed = new HashSet<String>();
		this.running = new HashSet<TaskThread>();
		if (numJobs <= 0)
			job_semaphore = new Semaphore(0) {
				@Override
				public void acquire() throws InterruptedException {
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
		this.targets = targets;
	}

	public void startTasks(Collection<String> tasks) {
		for (String task : tasks) {
			LOG.info("Starting " + task);
			DataTransformationRule t = this.tasks.get(task);
			TaskThread tt = new TaskThread(t, job_semaphore, lock, condition);
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
			for (String task : graph.getReadyForRunTasks(targets
					.toArray(new String[] {}))) {
				if (!runningNames.contains(task) && !failed.contains(task)) {
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
				LOG.error("Execution is interrupted");
				return;
			}

			Collection<TaskThread> justFinished = new ArrayList<TaskThread>();
			Set<TaskThread> stillRunning = new HashSet<TaskThread>();

			for (TaskThread t : running) {
				if (t.isFinished()) {
					justFinished.add(t);
				} else {
					stillRunning.add(t);
				}
			}
			for (TaskThread tt : justFinished) {
				if (tt.getReturnCode() == 0) {
					LOG.info("Execution of " + tt.getTaskName()
							+ " is completed");
					graph.removeTask(tt.getTaskName());
				} else {
					LOG.error("Execution of " + tt.getTaskName()
							+ " is failed with code " + tt.getReturnCode());
					removeDependentTasks(graph, failed, tt.getTaskName());
					failed.add(tt.getTaskName());
					graph.removeTask(tt.getTaskName());
					
				}
			}
			running = stillRunning;
			lock.unlock();
		}
	}
	
	private void removeDependentTasks(ExecutionGraph graph, Set<String> failed, String task){
		for(String dependentTask : graph.getDependentTasks(task)){
			LOG.warn("Removing dependent task " + dependentTask + " from execution graph");
			failed.add(dependentTask);
			graph.removeTask(dependentTask);
			removeDependentTasks(graph, failed, dependentTask);
		}
	}
}
