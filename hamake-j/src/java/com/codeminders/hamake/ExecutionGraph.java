package com.codeminders.hamake;

import java.util.List;

interface ExecutionGraph {

    List<String> getReadyForRunTasks();
    void removeTask(String name);
    
}
