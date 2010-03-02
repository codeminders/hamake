package com.codeminders.hamake;

import java.util.Collection;

interface ExecutionGraph {

    void addTasks(Collection<Task> tasks);
    void removeTask(String name);
    void setTargets(Collection<String> targets);

    Collection<String> getReadyForRunTasks();
}
