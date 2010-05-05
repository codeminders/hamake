package com.codeminders.hamake.dtr;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.data.DataFunction;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class Fold extends DataTransformationRule {

	public static final Log LOG = LogFactory.getLog(Fold.class);

	private List<? extends DataFunction> inputs = new ArrayList<DataFunction>();
	private List<? extends DataFunction> outputs = new ArrayList<DataFunction>();
	private List<? extends DataFunction> deps = new ArrayList<DataFunction>();
	private Context context;

	public Fold(Context parentContext, List<? extends DataFunction> inputs,
			List<? extends DataFunction> outputs, List<? extends DataFunction> deps) {
		this.inputs = inputs;
		this.outputs = outputs;
		this.deps = deps;
		this.context = new Context(parentContext);
	}

	@Override
	protected List<? extends DataFunction> getDeps() {
		return deps;
	}

	@Override
	protected List<? extends DataFunction> getInputs() {
		return inputs;
	}

	@Override
	protected List<? extends DataFunction> getOutputs() {
		return outputs;
	}
	
	@Override
	protected Context getContext() {
		return context;
	}

	@Override
	public int execute(Semaphore semaphore) throws IOException {
		long mits = -1;
		long mots = -1;

		int numo = 0;
		for (DataFunction func : outputs) {
			long stamp = func.getMinTimeStamp(context);
			if (stamp == 0) {
				mots = -1;
				break;
			}
			if (stamp > mots) {
				mots = stamp;
			}
			numo++;
		}
		if (numo > 0 && mots != -1) {
			for (DataFunction func : inputs) {
				long stamp = func.getMinTimeStamp(context);
				if (stamp == 0) {
					LOG.error("Some of input/dependency files not present!");
					return -10;
				}
				if (stamp > mits)
					mits = stamp;
			}
		}

		if (mits == -1 || mits > mots) {
			// check that input folder is not empty
			for (DataFunction func : inputs) {
				try {
					if (func.getPath(context).isEmpty()) {
						LOG.warn("WARN: The input folder is empty for task "
								+ getName());
					}
				} catch (IOException e) {
					LOG.error(e);
				}
			}
			for (DataFunction output : outputs)
				output.clear(context);

			return getTask().execute(context);
		}
		else{
			LOG.info("Output of " + getName()
					+ " is already present and fresh");
		}
		// all fresh
		return 0;
	}
	
}