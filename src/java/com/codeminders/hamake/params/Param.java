package com.codeminders.hamake.params;

import org.apache.hadoop.fs.FileSystem;

import com.codeminders.hamake.HamakePath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Param {

    List<String> get(Map<String, List<HamakePath>> dict, FileSystem fs) throws IOException;
}
