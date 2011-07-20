
package com.codeminders.hamake.task;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codeminders.hamake.Utils;
import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.params.Parameter;
import com.codeminders.hamake.params.SystemProperty;

public class Exec extends Task
{

    public static final Log LOG = LogFactory.getLog(Exec.class);

    private String          binary;

    public Exec()
    {
    }

    public int execute(Context context) throws IOException
    {
        Collection<String> args = new ArrayList<String>();
        args.add(binary);
        List<Parameter> parameters = getParameters();
        if(parameters != null)
        {
            for(Parameter p : parameters)
            {
                try
                {
                    if(p instanceof SystemProperty)
                    {
                        System.setProperty(((SystemProperty) p).getName(), ((SystemProperty) p).getValue());
                    } else
                    {
                        args.add(p.get(context));
                    }
                } catch(IOException ex)
                {
                    LOG.error("Failed to extract parameter values", ex);
                    return -1000;
                }
            }
        }
        String command = StringUtils.join(args, ' ');
        return Utils.execute(context, command);
    }

    public String getBinary()
    {
        return binary;
    }

    public void setBinary(String binary)
    {
        this.binary = binary;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append("binary", binary).appendSuper(super.toString()).toString();
    }

}