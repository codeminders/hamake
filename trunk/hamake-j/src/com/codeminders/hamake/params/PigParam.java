package com.codeminders.hamake.params;

import com.codeminders.hamake.Param;
import org.apache.hadoop.fs.FileSystem;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class PigParam implements Param {
    private String name;
    private String value;

    public PigParam(String name, String value) {
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

    public Collection<String> get(Map<String, Collection> dict, FileSystem fs) {
        return Collections.singleton(getValue());
    }
}