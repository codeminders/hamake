package com.codeminders.hamake;

import java.util.List;

interface ExecutionGraph {

    List<String> getReadyForRunTasks();
    public List<String> getReadyForRunTasks(String[] targets); 
    void removeTask(String name);
    
}
