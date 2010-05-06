package com.codeminders.hamake;

import java.io.IOException;

public interface Command {
    int execute(Context context) throws IOException;
}
