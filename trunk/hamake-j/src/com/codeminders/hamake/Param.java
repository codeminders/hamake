package com.codeminders.hamake;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface Param {

    Collection<String> get(Map<String, Collection> dict, FileSystem fs) throws IOException;
}
