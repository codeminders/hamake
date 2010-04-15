package com.codeminders.hamake.params;

import com.codeminders.hamake.HamakePath;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.fs.FileSystem;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConstParam implements Param {

    private String value;

    public ConstParam(String value) {
        setValue(value);
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

    @Override
     public String toString() {
         return new ToStringBuilder(this).append("value", value).toString();
     }
    
}
