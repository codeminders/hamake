package com.codeminders.hamake;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Command {
    int execute(Map<String, List<HamakePath>> parameters, Map<String, Object> context) throws IOException;
}
