package com.codeminders.hamake.params;

import org.apache.commons.io.FilenameUtils;

public class NormalizePathProcessingFunction extends ProcessingFunction {
	
	@Override
	public String process(String value) {
		if(value.matches("^[\"]?([A-Z]|[a-z]){1}[\\:]{1}([\\/]|[\\\\]){1}.*")){
			return FilenameUtils.normalize(value);
		}
		return value;
	}
}
