package com.codeminders.hamake.commands;

import com.codeminders.hamake.Config;
import com.codeminders.hamake.Param;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.ExitException;
import com.codeminders.hamake.params.JobConfParam;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.RunJar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class HadoopCommand extends BaseCommand {
    
	public static final Log LOG = LogFactory.getLog(HadoopCommand.class);
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
        	LOG.error("Can't download JAR file: " + getJar(), ex);
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
                    	LOG.error("Failed to extract parameter values from parameter", ex);
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
                    	LOG.error("Failed to extract parameter values from parameter", ex);
                        return -1000;
                    }
                }
            }
        }
        try {
            String s_args[] = new String[args.size()];
            args.toArray(s_args);
            if (Config.getInstance().verbose)
            	LOG.info("Executing Hadoop task " + StringUtils.join(s_args, ' '));
            if (Config.getInstance().dryrun)
                return 0;
            RunJar.main(s_args);            
        } catch (ExitException e){
            return e.status;
        } catch (Throwable ex) {
        	LOG.error("Failed to execute Hadoop command " + getJar() + '/' + getMain(), ex);
            return -1000;
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