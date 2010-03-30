package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import java.security.Permission;
import java.util.ArrayList;
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
    protected int numJobs;
    protected String projectName;
    public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	protected List<String> targets;
	protected List<Task> tasks;
	protected String defaultTarget;

    private FileSystem fileSystem;

    public Hamake() {
        this.tasks = new ArrayList<Task>();
        this.targets = new ArrayList<String>();
    }

    public void setNumJobs(int numJobs) {
        this.numJobs = numJobs;
    }

    public void addTask(Task task) {
        tasks.add(task);
    }
    
    public void setDefaultTarget(String targetName){
    	defaultTarget = targetName;
    }

    public void addTarget(String target) {
        this.targets.add(target);
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public ExitCode run() {
    	
    	SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());
        
        Map<String, Object> context = new HashMap<String, Object>();
        context.put("filesystem", fileSystem);
        try{
	        if(targets.size() <= 0 && !StringUtils.isEmpty(defaultTarget)){
	        	targets.add(defaultTarget);
	        }        
	        TaskRunner runner = new TaskRunner(tasks,
	                numJobs,
	                targets,
	                context);
	        runner.run();
	        if (runner.getFailed() > 0)
	            return ExitCode.FAILED;
        }
        finally{
        	System.setSecurityManager(securityManager);
        }
        return ExitCode.OK;
    }

	protected List<Task> getTasks() {
		return tasks;
	}
	
	private static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
            // allow anything.
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // allow anything.
        }

        @Override
        public void checkExit(int status) {
            super.checkExit(status);
            throw new ExitException(status);
        }
    }

}
