package com.codeminders.hamake.params;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.codeminders.hamake.context.Context;

import java.io.IOException;

public class JobConfParam implements Parameter{
    private String name;
    private String value;

    public JobConfParam(String name, String value) {
        setName(name);
        setValue(value);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String get(Context context) throws IOException {
        return "-jobconf " + getName() + '=' + getValue();
    }

    @Override
     public String toString() {
         return new ToStringBuilder(this).
                 append("name", name).
                 append("value", value).toString();
     }

}
