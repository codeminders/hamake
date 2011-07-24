package com.codeminders.hamake.params;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.codeminders.hamake.context.Context;
import org.apache.commons.lang.StringUtils;

public class HamakeParameter implements Parameter{
	
	public HamakeParameter(List<? extends ParameterItem> values, ConcatFunction concatFunc, ProcessingFunction processingFunc){
		this.values = values;
		this.concatFunc = concatFunc;
		this.processingFunc = processingFunc;
	}

	public HamakeParameter(String name, List<? extends ParameterItem> values, ConcatFunction concatFunc, ProcessingFunction processingFunc) {
        this(values, concatFunc, processingFunc);
        this.name = name;
    }

    private List<? extends ParameterItem> values;
	private ConcatFunction concatFunc;
	private ProcessingFunction processingFunc;
    private String name = null;
	
	public String get(Context context) throws IOException{
		List<String> parameters = new ArrayList<String>();
		for(Object value : values){
			if(value instanceof Reference){
				Reference reference = (Reference)value;
                String sv = processingFunc.process(reference.getValue(context, concatFunc));
                if (!StringUtils.isEmpty(sv))
				    parameters.add(sv);
			}
			else if(value instanceof Literal){
				Literal literal = (Literal)value;
                String sv = processingFunc.process(literal.getValue(context, concatFunc));
                if (!StringUtils.isEmpty(sv))
				    parameters.add(sv);
			}
		}
		return concatFunc.concat(parameters.toArray(new String[] {}));
	}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
