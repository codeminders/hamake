package com.codeminders.hamake;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
        // TODO
        //connectToDFS();
        try {
            Map<String, Object> context = new HashMap<String, Object>();
            // TODO
            //context.put("fsclient", fsclient);
            TaskRunner runner = new TaskRunner(getTasks(),
                    getNumJobs(),
                    getTargets(),
                    context);
            runner.run();
            if (runner.getFailures() > 0)
                    return ExitCode.FAILED;
            return ExitCode.OK;
        } finally {
            // TODO
            //if (transport != null)
            //    transport.close();
        }
    }

// TODO
    /*
    def connectToDFS(self):
        self.socket = TSocket.TSocket(self.thrift_host, self.thrift_port)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.fsclient = hadoopfs.ThriftHadoopFileSystem.Client(self.protocol)
        self.fsclient.mutex = threading.Lock()
        self.transport.open()
*/
}
