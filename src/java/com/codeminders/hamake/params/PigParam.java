package com.codeminders.hamake.params;

import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.NamedParam;
import org.apache.hadoop.fs.FileSystem;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PigParam implements NamedParam {
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

    public List<String> get(Map<String, List<HamakePath>> dict, FileSystem fs) {
        return Collections.unmodifiableList(Arrays.asList(getValue()));
    }
}