package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Hamake {

    enum ExitCode {
        OK,
        BADOPT,
        INITERR,
        FAILED
    }
    private int numJobs;
    private Collection<String> targets;
    private List<Task> tasks;
    private String startTask;

    private FileSystem fileSystem;

    public Hamake() {
        this.tasks = new ArrayList<Task>();
    }

    public String getStartTask() {
		return startTask;
	}

	public void setStartTask(String startTask) {
		if(StringUtils.isEmpty(this.startTask)){
			this.startTask = startTask;
		}
	}

	public int getNumJobs() {
        return numJobs;
    }

    public void setNumJobs(int numJobs) {
        this.numJobs = numJobs;
    }

    public void addTask(Task task) {
        getTasks().add(task);
    }

    public Collection<String> getTargets() {
        return targets;
    }

    public void setTargets(Collection<String> targets) {
        this.targets = targets;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public ExitCode run() {

        Map<String, Object> context = new HashMap<String, Object>();
        context.put("filesystem", getFileSystem());
        TaskRunner runner = new TaskRunner(getTasks(),
                getNumJobs(),
                getTargets(),
                context, startTask);
        runner.run();
        if (runner.getFailed() > 0)
            return ExitCode.FAILED;
        return ExitCode.OK;
    }

}
