package com.codeminders.hamake;

import java.util.Collection;

public interface Task {

    String getName();
    Collection<Path> getOutputs();
    
}
