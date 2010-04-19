package com.codeminders.hamake.params;

import org.apache.commons.lang.StringUtils;

public class CommaConcatFunction extends ConcatFunction {

	@Override
	public String concat(String... values) {
		return StringUtils.join(values, ",");
	}

}
