package com.codeminders.hamake.task;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.params.Parameter;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class Task{

    private List<Parameter> parameters = new ArrayList<Parameter>();
    
    public abstract int execute(Context context) throws IOException;
    
    public List<Parameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public void addParameter(Parameter param) {
        parameters.add(param);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("parameters", parameters).toString();
    }
}