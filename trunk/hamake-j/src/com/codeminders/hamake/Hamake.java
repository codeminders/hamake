package com.codeminders.hamake;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.net.InetSocketAddress;
import java.io.IOException;

public class Hamake {
    enum ExitCode {
        OK,
        BADOPT,
        INITERR,
        FAILED
    }
    private int numJobs;
    private Collection<String> targets;
    private Collection<Task> tasks;

    private String thriftHost;
    private int thriftPort;

    public Hamake() {
        tasks = new ArrayList<Task>();
    }

    public int getNumJobs() {
        return numJobs;
    }

    public void setNumJobs(int numJobs) {
        this.numJobs = numJobs;
    }

    public void addTask(Task task) {
        getTasks().add(task);
    }

    public Collection<String> getTargets() {
        return targets;
    }

    public void setTargets(Collection<String> targets) {
        this.targets = targets;
    }

    public Collection<Task> getTasks() {
        return tasks;
    }

    public void setTasks(Collection<Task> tasks) {
        this.tasks = tasks;
    }

    public String getThriftHost() {
        return thriftHost;
    }

    public void setThriftHost(String thriftHost) {
        this.thriftHost = thriftHost;
    }

    public int getThriftPort() {
        return thriftPort;
    }

    public void setThriftPort(int thriftPort) {
        this.thriftPort = thriftPort;
    }

    public ExitCode run() {

        InetSocketAddress address = new InetSocketAddress(getThriftHost(), getThriftPort());
        Configuration config = new Configuration();

        DFSClient fsclient;

        try {
            fsclient = new DFSClient(address, config);
        } catch (IOException ex) {
            System.err.println("Unable to connect to DFS: " + ex.getMessage());
            if (Config.getInstance().test_mode)
                ex.printStackTrace();
            return ExitCode.FAILED;
        }
        Map<String, Object> context = new HashMap<String, Object>();
        context.put("fsclient", fsclient);
        TaskRunner runner = new TaskRunner(getTasks(),
                getNumJobs(),
                getTargets(),
                context);
        runner.run();
        if (runner.getFailed() > 0)
            return ExitCode.FAILED;
        return ExitCode.OK;
    }

}
