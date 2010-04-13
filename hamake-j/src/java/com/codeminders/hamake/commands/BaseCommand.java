package com.codeminders.hamake.commands;

import com.codeminders.hamake.Command;
import com.codeminders.hamake.Param;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseCommand implements Command {

    private List<Param> parameters = new ArrayList<Param>();
    
    public List<Param> getParameters() {
        return parameters;
    }

    public void setParameters(List<Param> parameters) {
        this.parameters = parameters;
    }

    public void addParameter(Param param) {
        parameters.add(param);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("parameters", parameters).toString();
    }
}