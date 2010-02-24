package com.codeminders.hamake;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface Param {

    // TODO: signature of fsClient?
    Collection<String> get(Map<String, List> dict, Object fsClient);
}
