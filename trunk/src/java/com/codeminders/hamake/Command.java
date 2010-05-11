package com.codeminders.hamake;

import java.io.IOException;

import com.codeminders.hamake.context.Context;

public interface Command {
    int execute(Context context) throws IOException;
}
