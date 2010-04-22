package com.codeminders.hamake.commands;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.RunJar;

import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import com.codeminders.hamake.*;
import com.codeminders.hamake.params.PigParam;
import com.codeminders.hamake.params.PathParam;
import com.codeminders.hamake.params.Param;

public class PigJarCommand extends BaseCommand {

    public static final Log LOG = LogFactory.getLog(PigJarCommand.class);

    private HamakePath script;
    private String jar;

    public PigJarCommand() {
        super();
    }

    public PigJarCommand(String jar, HamakePath scriptPath, List<Param> params) {
        LOG.info("running PIG from jar " + jar);
        setJar(jar);
        setScript(scriptPath);
        setParameters(params);
    }

    @SuppressWarnings("unchecked")
    public int execute(Map<String, List<HamakePath>> parameters, Map<String, Object> context) {
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

        Collection<Param> scriptParams = getParameters();
        if (scriptParams != null) {
            for (Param p : scriptParams) {
                if (p instanceof PigParam || p instanceof PathParam) {
                    Collection<String> values = getValues((NamedParam)p, parameters, fs);
                    if (values == null) return -1000;
                    args.add("-p");
                    args.add(((NamedParam)p).getName() + '=' + values.iterator().next());
                }
            }
        }

        try {
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

    public HamakePath getScript() {
        return script;
    }

    public void setScript(HamakePath script) {
        this.script = script;
    }

    protected Collection<String> getValues(NamedParam p, Map<String, List<HamakePath>> parameters, FileSystem fs)
    {
        Collection<String> values;
        try {
            values = p.get(parameters, fs);
        } catch (IOException ex) {
         LOG.error("Failed to extract parameter values from parameter " +
                    p.getName(), ex);
            return null;
        }
        if (values.size() != 1) {
         LOG.error("Multiple values for param are no supported in PIG scripts");
            return null;
        }

        return values;
    }
}
