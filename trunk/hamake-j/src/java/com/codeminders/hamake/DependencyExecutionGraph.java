package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;

import java.util.*;

class DependencyExecutionGraph implements ExecutionGraph {

    private Map<String, Set<String>> tasks;

    public DependencyExecutionGraph(Collection<Task> tasks, Collection<String> targets) {
        this.tasks = new HashMap<String, Set<String>>();
        addTasks(tasks);

        if (targets != null && targets.size() > 0)
            setTargets(targets);
    }

    public void addTasks(Collection<Task> tasks) {
        for (Task t : tasks) {
            Set<String> deps = new HashSet<String>();
            for (Task ot : tasks) {
                if (t.dependsOn(ot))
                    deps.add(ot.getName());
            }
            this.tasks.put(t.getName(), deps);
        }
    }

    public void removeTask(String name) {
        tasks.remove(name);
        for (Map.Entry<String, Set<String>> entry : tasks.entrySet()) {
            entry.getValue().remove(name);
        }
    }

    public void setTargets(Collection<String> targets) {
        // Clean up dependency graph to build only given targets and tasks they depend on
        if (Config.getInstance().test_mode)
            System.err.println("Settings targets: " + StringUtils.join(targets, ' '));
        while (true) {
            boolean changed = false;
            for (Map.Entry<String, Set<String>> entry : tasks.entrySet()) {
                if (targets.contains(entry.getKey())) {
                    for (String dep : entry.getValue()) {
                        if (!targets.contains(dep)) {
                            targets.add(dep);
                            if (Config.getInstance().verbose) {
                                System.out.println("Addint task " + dep + " to build list since " + entry.getKey() + " depends on it");
                            }
                            changed = true;
                        }
                    }
                }
            }
            if (!changed)
                break;
        }

        if (Config.getInstance().test_mode) {
            System.err.println("Final list of targets: " + StringUtils.join(targets, ' '));
        }
        Map<String, Set<String>> newtasks = new HashMap<String, Set<String>>();
        for (Map.Entry<String, Set<String>> entry : tasks.entrySet()) {
            if (targets.contains(entry.getKey())) {
                newtasks.put(entry.getKey(), entry.getValue());
            }
        }
        this.tasks = newtasks;
    }

    public List<String> getReadyForRunTasks() {
        List<String> ret = new ArrayList<String>();
        for (Map.Entry<String, Set<String>> entry : tasks.entrySet()) {
            if (entry.getValue().isEmpty()) {
                ret.add(entry.getKey());
            }
        }
        return ret;
    }
}

