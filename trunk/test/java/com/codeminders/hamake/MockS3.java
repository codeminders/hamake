package com.codeminders.hamake;

import java.net.URI;

public class MockS3 extends MockFS{

	public static final String DEFAULT_URL = "s3:///";
	
	static final URI NAME = URI.create(DEFAULT_URL);

	@Override
	public URI getUri() {
		return NAME;
	}
	
}
