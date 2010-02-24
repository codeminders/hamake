package com.codeminders.hamake;

import java.util.List;
import java.util.Map;

public interface Command {
    int execute(Map<String, List> parameters, Map<String, Object> context);
}
