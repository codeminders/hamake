package com.codeminders.hamake.commands;

import com.codeminders.hamake.Config;
import com.codeminders.hamake.Param;
import com.codeminders.hamake.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class ExecCommand extends BaseCommand {
	
	public static final Log LOG = LogFactory.getLog(ExecCommand.class);

    private String binary;

    public ExecCommand() {
    }

    public ExecCommand(String binary, Collection<Param> parameters) {
        setBinary(binary);
        setParameters(parameters);
    }

    public int execute(Map<String, Collection> parameters, Map<String, Object> context) {
        FileSystem fs = Utils.getFileSystem(context);
        Collection<String> args = new ArrayList<String>();
        args.add(getBinary());
        Collection<Param> scriptParams = getParameters();
        if (scriptParams != null) {
            for (Param p : scriptParams) {
                try {
                    args.addAll(p.get(parameters, fs));
                } catch (IOException ex) {
                	LOG.error("Failed to extract parameter values", ex);
                    return -1000;
                }
            }
        }
        String command = StringUtils.join(args, ' ');
        if (Config.getInstance().dryrun)
            return 0;
        return Utils.execute(command);
    }

    public String getBinary() {
        return binary;
    }

    public void setBinary(String binary) {
        this.binary = binary;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("binary", binary).appendSuper(super.toString()).toString();
    }

}