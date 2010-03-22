package com.codeminders.hamake;

import java.util.List;

interface ExecutionGraph {

    List<String> getReadyForRunTasks();
    List<String> getReadyForRunTasks(String rootTask); 
    void removeTask(String name);
    
}
