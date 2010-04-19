package com.codeminders.hamake.params;

import org.apache.commons.lang.StringUtils;

public class SpaceConcatFunction extends ConcatFunction {

	@Override
	public String concat(String... values) {
		return StringUtils.join(values, " ");
	}

}
