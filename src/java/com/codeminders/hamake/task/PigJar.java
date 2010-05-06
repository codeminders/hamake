package com.codeminders.hamake.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.RunJar;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import com.codeminders.hamake.*;
import com.codeminders.hamake.params.Parameter;
import com.codeminders.hamake.params.HamakeParameter;

public class PigJar extends Task {

    public static final Log LOG = LogFactory.getLog(PigJar.class);

    private Path script;
    private String jar;

    public PigJar() {
        super();
    }

    public PigJar(String jar, Path scriptPath, List<Parameter> params) {
        LOG.info("running PIG from jar " + jar);
        setJar(jar);
        setScript(scriptPath);
        setParameters(params);
    }

    public int execute(Context context) {
        FileSystem fs;
        Collection<String> args = new ArrayList<String>();
        try {
            Path jarPath = new Path(getJar());
            fs = jarPath.getFileSystem(new Configuration());
            args.add(Utils.copyToTemporaryLocal(getJar(), fs).getAbsolutePath());
        } catch (IOException ex) {
            LOG.error("Can't download JAR file: " + getJar(), ex);
            return -1000;
        }

        args.add("-f");
        args.add(script.toString());

        try {
            List<Parameter> parameters = getParameters();
            if (parameters != null) {
                for (Parameter p : parameters) {
                    if (p instanceof HamakeParameter) {
                        args.add("-p");
                        args.add(((HamakeParameter)p).getName() + '=' + p.get(context));
                    }
                }
            }

            String s_args[] = new String[args.size()];
            args.toArray(s_args);
            if (Config.getInstance().verbose)
                LOG.info("Executing PIG task " + StringUtils.join(s_args, ' '));
            if (Config.getInstance().dryrun)
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

    public String getJar() {
        return jar;
    }

    public void setJar(String jar) {
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
