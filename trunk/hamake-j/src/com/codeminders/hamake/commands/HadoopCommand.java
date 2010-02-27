package com.codeminders.hamake.commands;

import com.codeminders.hamake.Config;
import com.codeminders.hamake.Param;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.params.JobConfParam;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.util.RunJar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class HadoopCommand extends BaseCommand {


    private String jar;
    private String main;

    public int execute(Map<String, Collection> parameters, Map<String, Object> context) {
        DFSClient fsclient = Utils.getFSClient(context);
        Collection<String> args = new ArrayList<String>();
        args.add(getJar());
        args.add(getMain());
        Collection<Param> scriptParams = getParameters();
        if (scriptParams != null) {
            // first add jobconf params
            for (Param p : scriptParams) {
                if (p instanceof JobConfParam) {
                    try {
                        args.addAll(p.get(parameters, fsclient));
                    } catch (IOException ex) {
                        System.err.println("Failed to extract parameter values from parameter: " + ex.getMessage());
                        if (Config.getInstance().test_mode)
                            ex.printStackTrace();
                        return -1000;
                    }
                }
            }
            // then the rest
            for (Param p : scriptParams) {
                if (!(p instanceof JobConfParam)) {
                    try {
                        args.addAll(p.get(parameters, fsclient));
                    } catch (IOException ex) {
                        System.err.println("Failed to extract parameter values from parameter: " + ex.getMessage());
                        if (Config.getInstance().test_mode)
                            ex.printStackTrace();
                        return -1000;
                    }
                }
            }
        }
        try {
            String s_args[] = new String[args.size()];
            args.toArray(s_args);
            RunJar.main(s_args);
            return 0;
        } catch (Throwable ex) {
            System.err.println("Failed to execute Hadoop command " + getJar() + '/' + getMain() + ": " + ex.getMessage());
            if (Config.getInstance().test_mode)
                ex.printStackTrace();
            return -1000;
        }
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