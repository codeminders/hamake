package com.codeminders.hamake;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface Command {
    int execute(Map<String, Collection> parameters, Map<String, Object> context) throws IOException;
}
