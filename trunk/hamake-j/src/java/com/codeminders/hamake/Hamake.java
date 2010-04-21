package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.codeminders.hamake.dtr.DataTransformationRule;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Hamake {
	
	public static final String SYS_PROPERTY_WORKING_FOLDER = "workdir";
	public static final String SYS_PROPERTY_HADOOP_CONFIGURATION = "hadoop.configuration";

    enum ExitCode {
        OK,
        BADOPT,
        INITERR,
        FAILED
    }
    protected int numJobs;
    protected String projectName;
    protected Context context;
    public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	protected List<String> targets;
	protected List<DataTransformationRule> tasks;
	protected String defaultTarget;

    public Hamake() {
        this.tasks = new ArrayList<DataTransformationRule>();
        this.targets = new ArrayList<String>();
    }

    public void setNumJobs(int numJobs) {
        this.numJobs = numJobs;
    }
    
    public void setContext(Context context) {
		this.context = context;
	}

	public void addTask(DataTransformationRule task) {
        tasks.add(task);
    }
    
    public void setDefaultTarget(String targetName){
    	defaultTarget = targetName;
    }

    public void addTarget(String target) {
        this.targets.add(target);
    }

    public void setTasks(List<DataTransformationRule> tasks) {
        this.tasks = tasks;
    }

    public ExitCode run() throws IOException {
    	
    	SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());        
        context.setSystem(SYS_PROPERTY_HADOOP_CONFIGURATION, new Configuration());
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

	protected List<DataTransformationRule> getTasks() {
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
