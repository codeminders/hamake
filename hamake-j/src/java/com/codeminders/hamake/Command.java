package com.codeminders.hamake;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Command {
    int execute(Context context) throws IOException;
}
