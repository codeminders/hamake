package com.codeminders.hamake.commands;

import com.codeminders.hamake.Param;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.params.PigParam;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PigCommand extends BaseCommand {

    /**
     * Default name of pig executable
     */
    public static final String PIGCMD = "pig";

    /**
     * Name of env. var. which holds name of pig executable
     */
    public static final String PIGCMDENV = "PIG";

    private String script;

    public PigCommand() {
        this(null);
    }
    
    public PigCommand(String script) {
        this(script, new ArrayList<Param>());
    }

    public PigCommand(String script, Collection<Param> parameters) {
        setScript(script);
        setParameters(parameters);
    }

    public int execute(Map<String, List> parameters, Map<String, Object> context) {
        Object fsClient = Utils.getFSClient(context);
        Collection<String> args = new ArrayList<String>();
        args.add(Utils.getenv(PIGCMDENV, PIGCMD));

        Collection<Param> scriptParams = getParameters();
        if (scriptParams != null) {
            for (Param p : scriptParams) {
                if (p instanceof PigParam) {
                    PigParam pp = (PigParam) p;
                    Collection<String> values = p.get(parameters, fsClient);
                    if (values.size() != 1) {
                        System.err.println("Multiple values for param are no supported in PIG scripts");
                        return -1000;
                    }
                    args.add("-param");
                    args.add(pp.getName() + '=' + values.iterator().next());
                }
            }
        }
        args.add("-f");
        args.add(getScript());
        return Utils.execute(StringUtils.join(args, ' '));
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    @Override
     public String toString() {
         return new ToStringBuilder(this).append("script", script).appendSuper(super.toString()).toString();
     }
    
}
