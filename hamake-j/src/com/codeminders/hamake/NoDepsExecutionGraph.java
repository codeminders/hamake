package com.codeminders.hamake;

import java.util.*;

class NoDepsExecutionGraph implements ExecutionGraph {

    private Map<String, Set<String>> tasks;
    private Collection<String> targets;

    public NoDepsExecutionGraph(Collection<Task> tasks, Collection<String> targets) {
        this.tasks = new HashMap<String, Set<String>>();
        setTargets(targets);
        addTasks(tasks);
    }

    public void addTasks(Collection<Task> tasks) {
        for (Task t : tasks) {
            if (targets.contains(t.getName())) {
                this.tasks.put(t.getName(), new HashSet<String>());
            }
        }
    }

    public void removeTask(String name) {
        tasks.remove(name);
        for (Map.Entry<String, Set<String>> entry : tasks.entrySet()) {
            entry.getValue().remove(name);
        }
    }

    public void setTargets(Collection<String> targets) {
        this.targets = new ArrayList<String>(targets);
    }

    public Collection<String> getReadyForRunTasks() {
        Collection<String> ret = new ArrayList<String>();
        for (Map.Entry<String, Set<String>> entry : tasks.entrySet()) {
            if (entry.getValue().isEmpty()) {
                ret.add(entry.getKey());
            }
        }
        return ret;
    }
}