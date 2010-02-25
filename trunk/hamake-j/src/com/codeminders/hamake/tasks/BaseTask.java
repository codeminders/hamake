package com.codeminders.hamake.tasks;

import com.codeminders.hamake.Command;
import com.codeminders.hamake.Path;
import com.codeminders.hamake.Task;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.Collection;

public abstract class BaseTask implements Task {

    private String name;
    private Collection<String> taskDeps = new ArrayList<String>();
    private Command command;
    private Collection<Path> outputs = new ArrayList<Path>();

    public String getName() {
        return name;
    }

    public abstract Collection<Path> getInputs();


    public Collection<Path> getOutputs() {
        return outputs;
    }

    public boolean dependsOn(Task t) {
        if (getTaskDeps().contains(t.getName()))
            return true;
        for (Path i : getInputs()) {
            for (Path o : t.getOutputs()) {
                if (i.intersects(o))
                    return true;
            }
        }

        return false;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public void setOutputs(Collection<Path> outputs) {
        this.outputs = outputs;
    }

    public Collection<String> getTaskDeps() {
        return taskDeps;
    }

    public void setTaskDeps(Collection<String> taskDeps) {
        this.taskDeps = taskDeps;
    }

    @Override
      public String toString() {
          return new ToStringBuilder(this).
                  append("name", name).
                  append("command", command).
                  append("outputs", outputs).
                  append("taskdeps", taskDeps).toString();
      }
    
}
