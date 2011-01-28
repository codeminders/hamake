package com.codeminders.hamake;

import java.io.IOException;
import java.io.InputStream;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Manifest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.context.ContextAware;
import com.codeminders.hamake.dtr.DataTransformationRule;

public class Hamake extends ContextAware{
	
	public static String HAMAKE_VERSION;
	
	static{
		try {
			InputStream manifestIS = Hamake.class.getResourceAsStream("../../../META-INF/MANIFEST.MF");
			if(manifestIS != null){
				Manifest manifest = new Manifest(manifestIS);
				HAMAKE_VERSION = manifest.getMainAttributes().getValue("Implementation-Version");
			}
		} catch (IOException e) {
			HAMAKE_VERSION = System.getProperty("hamake.version");
		}
		HAMAKE_VERSION = (HAMAKE_VERSION == null) ? System.getProperty("hamake.version") : HAMAKE_VERSION;
	}
	
	public static final Log LOG = LogFactory.getLog(Hamake.class);
	
	public enum ExitCode {
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
	protected List<DataTransformationRule> tasks;
	protected String defaultTarget;

    public Hamake(Context rootContext) throws InvalidContextStateException {
    	super(rootContext);
        this.tasks = new ArrayList<DataTransformationRule>();
        this.targets = new ArrayList<String>();
    }

    public void setNumJobs(int numJobs) {
        this.numJobs = numJobs;
    }
    
	public void addTask(DataTransformationRule task) {
        tasks.add(task);
    }
    
    public void setDefaultTarget(String targetName){
    	defaultTarget = targetName;
    }
    
    /**
     * For unit tests
     * @return default target name
     */
    public String getDefaultTarget() {
		return defaultTarget;
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
        try{
	        if(targets.size() <= 0 && !StringUtils.isEmpty(defaultTarget)){
	        	targets.add(defaultTarget);
	        }        
	        TaskRunner runner = new TaskRunner(tasks,
	                numJobs,
	                targets);
	        runner.run();
	        if (runner.getFailed() > 0)
	            return ExitCode.FAILED;
        } catch(Exception e){
        	LOG.error("An error occured", e);
        	return ExitCode.FAILED;
        } finally{
        	EhCacheListener.removeAll(Context.cacheManager.getCache("mapReduceJarCache"));
        	EhCacheListener.removeAll(Context.cacheManager.getCache("MapReduceUnpackedJarCache"));
        	System.setSecurityManager(securityManager);
        }
        return ExitCode.OK;
    }

	public List<DataTransformationRule> getTasks() {
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
            LOG.warn("Some of your tasks have called System.exit() method. This is not recommended behaviour.");
            throw new ExitException(status);
        }
    }

}
