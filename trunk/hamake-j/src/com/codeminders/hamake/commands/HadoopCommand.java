package com.codeminders.hamake.commands;

import com.codeminders.hamake.Param;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.params.JobConfParam;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HadoopCommand extends BaseCommand {


    /**
     * Default name of hadoop executable
     */
    public static final String HADOOPCMD = "hadoop";

    /**
     * Name of env. var. which holds name of hadoop executable
     */
    public static final String HADOOPCMDENV = "HADOOP";

    private String jar;
    private String main;

    public int execute(Map<String, List> parameters, Map<String, Object> context) {
        Object fsclient = Utils.getFSClient(context);
        Collection<String> args = new ArrayList<String>();
        args.add(Utils.getenv(HADOOPCMDENV, HADOOPCMD));
        args.add("jar");
        args.add(getJar());
        args.add(getMain());
        Collection<Param> scriptParams = getParameters();
        if (scriptParams != null) {
            // first add jobconf params
            for (Param p : scriptParams) {
                if (p instanceof JobConfParam)
                    args.addAll(p.get(parameters, fsclient));
            }
            // then the rest
            for (Param p : scriptParams) {
                if (!(p instanceof JobConfParam))
                    args.addAll(p.get(parameters, fsclient));
            }
        }
        String command = StringUtils.join(args, ' ');
        return Utils.execute(command);
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