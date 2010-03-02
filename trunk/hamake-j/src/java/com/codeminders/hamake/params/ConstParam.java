package com.codeminders.hamake.params;

import com.codeminders.hamake.Param;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.fs.FileSystem;

import java.util.Collection;
import java.util.Collections;
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

    public Collection<String> get(Map<String, Collection> dict, FileSystem fs) {
        return Collections.singleton(getValue());
    }

    @Override
     public String toString() {
         return new ToStringBuilder(this).append("value", value).toString();
     }
    
}
