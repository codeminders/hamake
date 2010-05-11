package com.codeminders.hamake.task;

import com.codeminders.hamake.Utils;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.params.Parameter;
import com.codeminders.hamake.params.SystemProperty;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Exec extends Task {
	
	public static final Log LOG = LogFactory.getLog(Exec.class);

    private Path binary;

    public Exec() {
    }

    public int execute(Context context) throws IOException {
        Collection<String> args = new ArrayList<String>();
        args.add(binary.toString());
        List<Parameter> parameters = getParameters();
        if (parameters != null) {
            for (Parameter p : parameters) {
                try {
                	if(p instanceof SystemProperty){
                    	System.setProperty(((SystemProperty)p).getName(), ((SystemProperty)p).getValue());
                    }
                	else{
                		args.add(p.get(context));
                	}
                } catch (IOException ex) {
                	LOG.error("Failed to extract parameter values", ex);
                    return -1000;
                }
            }
        }
        String command = StringUtils.join(args, ' ');
        if (context.getBoolean(Context.HAMAKE_PROPERTY_DRY_RUN))
            return 0;
        return Utils.execute(context, command);
    }

    public Path getBinary() {
        return binary;
    }

    public void setBinary(Path binary) {
        this.binary = binary;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("binary", binary).appendSuper(super.toString()).toString();
    }
    
}