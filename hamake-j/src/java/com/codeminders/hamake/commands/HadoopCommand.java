package com.codeminders.hamake.commands;

import com.codeminders.hamake.Config;
import com.codeminders.hamake.Param;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.params.JobConfParam;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.RunJar;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class HadoopCommand extends BaseCommand {
	
	protected static class ExitException extends SecurityException {
	    private static final long serialVersionUID = -1982617086752946683L;
	    public final int status;

	    public ExitException(int status) {
	        super();
	        this.status = status;
	    }
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

    private String jar;
    private String main;

    public int execute(Map<String, Collection> parameters, Map<String, Object> context) {
        FileSystem fs;
        Collection<String> args = new ArrayList<String>();
        try {
            Path jarPath = new Path(getJar());             
            fs = Utils.getFileSystem(context, jarPath.toUri());
            args.add(Utils.copyToTemporaryLocal(getJar(), fs).getAbsolutePath());
        } catch (IOException ex) {
            System.err.println("Can't download JAR file: " + getJar());
            if (Config.getInstance().test_mode)
                ex.printStackTrace();
            return -1000;
        }
        args.add(getMain());
        Collection<Param> scriptParams = getParameters();
        if (scriptParams != null) {
            // first add jobconf params
            for (Param p : scriptParams) {
                if (p instanceof JobConfParam) {
                    try {
                        args.addAll(p.get(parameters, fs));
                    } catch (IOException ex) {
                        System.err.println("Failed to extract parameter values from parameter: " + ex.getMessage());
                        if (Config.getInstance().test_mode)
                            ex.printStackTrace();
                        return -1000;
                    }
                }
            }
            // then the rest
            for (Param p : scriptParams) {
                if (!(p instanceof JobConfParam)) {
                    try {
                        args.addAll(p.get(parameters, fs));
                    } catch (IOException ex) {
                        System.err.println("Failed to extract parameter values from parameter: " + ex.getMessage());
                        if (Config.getInstance().test_mode)
                            ex.printStackTrace();
                        return -1000;
                    }
                }
            }
        }
        SecurityManager securityManager = System.getSecurityManager();
        try {
            String s_args[] = new String[args.size()];
            args.toArray(s_args);
            if (Config.getInstance().verbose)
                System.err.println("Executing Hadoop task " + StringUtils.join(s_args, ' '));
    		System.setSecurityManager(new NoExitSecurityManager());
            RunJar.main(s_args);            
        } catch (ExitException e){
        	System.out.println("System.exit(" + e.status + ") has been called from " + main + " It is reccommended not to call this method whithin Hadoop Jobs");        	
        } catch (Throwable ex) {
            System.err.println("Failed to execute Hadoop command " + getJar() + '/' + getMain() + ": " + ex.getMessage());
            if (Config.getInstance().test_mode)
                ex.printStackTrace();
            return -1000;
        }
        finally{
        	System.setSecurityManager(securityManager);
        }
        return 0;
    }

    public String getJar() {
        return jar;
    }

    public void setJar(String jar) {
        this.jar = jar;
    }

    public String getMain() {
        return main;
    }

    public void setMain(String main) {
        this.main = main;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("jar", jar).
                append("main", main).appendSuper(super.toString()).toString();
    }

}