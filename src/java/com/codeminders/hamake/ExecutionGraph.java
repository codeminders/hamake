package com.codeminders.hamake;

import java.util.List;

interface ExecutionGraph {

    List<String> getReadyForRunTasks();
    public List<String> getReadyForRunTasks(String[] targets);
    public List<String> getDependentTasks(String task);
    void removeTask(String name);
    
}
