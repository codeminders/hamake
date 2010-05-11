package com.codeminders.hamake.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.RunJar;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.io.File;

import com.codeminders.hamake.*;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.params.Parameter;
import com.codeminders.hamake.params.HamakeParameter;
import com.codeminders.hamake.params.SystemProperty;

public class PigJar extends Task {

    public static final Log LOG = LogFactory.getLog(PigJar.class);

    private Path script;
    private File jar;

    public PigJar() {
        super();
    }

    public PigJar(File jar, Path scriptPath, List<Parameter> params) {
        LOG.info("running PIG from jar " + jar);
        setJar(jar);
        setScript(scriptPath);
        setParameters(params);
    }

    public int execute(Context context) {
        Collection<String> args = new ArrayList<String>();
        args.add(getJar().getAbsolutePath());

        args.add("-f");
        if(!script.toString().startsWith("file:")) args.add(script.toString());
        else args.add(script.toUri().getPath().toString());
        try {
            List<Parameter> parameters = getParameters();
            if (parameters != null) {
                for (Parameter p : parameters) {
                    if (p instanceof HamakeParameter) {
                        args.add("-p");
                        args.add(((HamakeParameter)p).getName() + '=' + p.get(context));
                    }
                    else if(p instanceof SystemProperty){
                    	System.setProperty(((SystemProperty)p).getName(), ((SystemProperty)p).getValue());
                    }
                }
            }

            String s_args[] = new String[args.size()];
            args.toArray(s_args);
            if (context.getBoolean(Context.HAMAKE_PROPERTY_VERBOSE))
                LOG.info("Executing PIG task " + StringUtils.join(s_args, ' '));
            if (context.getBoolean(Context.HAMAKE_PROPERTY_DRY_RUN))
                return 0;
            RunJar.main(s_args);
        } catch (ExitException e){
            return e.status;
        } catch (Throwable ex) {
            LOG.error("Failed to execute PIG command " + getJar(), ex);
            return -1000;
        }
        return 0;
    }

    public File getJar() {
        return jar;
    }

    public void setJar(File jar) {
        this.jar = jar;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("jar", jar).appendSuper(super.toString()).toString();
    }

    public Path getScript() {
        return script;
    }

    public void setScript(Path script) {
        this.script = script;
    }
}
