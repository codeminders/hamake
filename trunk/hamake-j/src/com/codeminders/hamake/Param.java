package com.codeminders.hamake;

import org.apache.hadoop.hdfs.DFSClient;

import java.util.Collection;
import java.util.Map;

public interface Param {

    Collection<String> get(Map<String, Collection> dict, DFSClient fsClient);
}
