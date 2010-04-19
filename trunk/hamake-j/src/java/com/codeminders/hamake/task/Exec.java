package com.codeminders.hamake.task;

import com.codeminders.hamake.Config;
import com.codeminders.hamake.Context;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.params.HamakeParameter;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Exec extends Task {
	
	public static final Log LOG = LogFactory.getLog(Exec.class);

    private HamakePath binary;

    public Exec() {
    }

    public Exec(HamakePath binary) {
        setBinary(binary);
    }

    public int execute(Context context) throws IOException {
        Collection<String> args = new ArrayList<String>();
        args.add(getBinary().getPathName().toString());
        List<HamakeParameter> parameters = getParameters();
        if (parameters != null) {
            for (HamakeParameter p : parameters) {
                try {
                    args.add(p.get(context));
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

    public HamakePath getBinary() {
        return binary;
    }

    public void setBinary(HamakePath binary) {
        this.binary = binary;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("binary", binary).appendSuper(super.toString()).toString();
    }

}