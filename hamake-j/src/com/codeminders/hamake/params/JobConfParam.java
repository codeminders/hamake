package com.codeminders.hamake.params;

import com.codeminders.hamake.Param;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hdfs.DFSClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class JobConfParam implements Param {
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

    public Collection<String> get(Map<String, Collection> dict, DFSClient fsClient) {
        Collection<String> ret = new ArrayList<String>();
        ret.add("-jobconf");
        ret.add(getName() + '=' + getValue());
        return ret;
    }

    @Override
     public String toString() {
         return new ToStringBuilder(this).
                 append("name", name).
                 append("value", value).toString();
     }

}
