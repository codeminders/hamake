package com.codeminders.hamake;

import java.util.List;
import java.util.Set;

interface ExecutionGraph {

    Set<String> getReadyForRunTasks();
    public Set<String> getReadyForRunTasks(String[] targets);
    public List<String> getDependentTasks(String task);
    void removeTask(String name);
    
}
