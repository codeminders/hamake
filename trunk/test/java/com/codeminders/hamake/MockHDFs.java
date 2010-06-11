package com.codeminders.hamake;

import java.net.URI;

public class MockHDFs extends MockFS {
	
	public static final String DEFAULT_URL = "hdfs://localhost:9000";
	
	static final URI NAME = URI.create(DEFAULT_URL);

	public MockHDFs() {
	}

	@Override
	public URI getUri() {
		return NAME;
	}

}