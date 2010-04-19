package com.codeminders.hamake.task;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.params.HamakeParameter;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class Task{

    private List<HamakeParameter> parameters = new ArrayList<HamakeParameter>();
    
    public abstract int execute(Context context) throws IOException;
    
    public List<HamakeParameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<HamakeParameter> parameters) {
        this.parameters = parameters;
    }

    public void addParameter(HamakeParameter param) {
        parameters.add(param);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("parameters", parameters).toString();
    }
}