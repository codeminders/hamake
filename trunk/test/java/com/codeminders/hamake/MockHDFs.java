package com.codeminders.hamake;

import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.io.IOException;

public class MockHDFs extends MockFS {
	
	public static final String DEFAULT_URL = "hdfs://localhost:9000";
	
	static final URI NAME = URI.create(DEFAULT_URL);

	public MockHDFs() {
	}

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        //TODO: implement it for 0.18
    }

    @Override
	public URI getUri() {
		return NAME;
	}

}