package com.codeminders.hamake;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;

public interface Task {

    String getName();
    Collection<Path> getOutputs();
    int execute(Semaphore semaphore, Map<String, Object> context) throws IOException;
    boolean dependsOn(Task t);

}
