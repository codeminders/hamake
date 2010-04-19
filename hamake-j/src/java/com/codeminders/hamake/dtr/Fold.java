package com.codeminders.hamake.dtr;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.Utils;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class Fold extends DataTransformationRule {
	
	public static final Log LOG = LogFactory.getLog(Fold.class);
	
	private List<HamakePath> inputs = new ArrayList<HamakePath>();
	private List<HamakePath> outputs = new ArrayList<HamakePath>();
	private List<HamakePath> deps = new ArrayList<HamakePath>();
	private Context context;

	public Fold(Context parentContext, List<HamakePath> inputs, List<HamakePath> outputs, List<HamakePath> deps){
		this.inputs = inputs;
		this.outputs = outputs;
		this.deps = deps;
		this.context = new Context(parentContext);
	}
	
	@Override
	protected List<HamakePath> getDeps() {
		return deps;
	}

	@Override
	protected List<HamakePath> getInputs() {
		return inputs;
	}

	@Override
	protected List<HamakePath> getOutputs() {
		return outputs;
	}
	
	@Override
    public String toString() {
        return new ToStringBuilder(this).
                append("inputs", inputs).appendSuper(super.toString()).toString();
    }

	@Override
    public int execute(Semaphore semaphore) throws IOException {
        long mits = -1;
        long mots = -1;                
        
        int numo = 0;
        for (HamakePath p : inputs) {
            long stamp = getTimeStamp(p.getFileSystem(), p);
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
            Collection<HamakePath> paths = new ArrayList<HamakePath>(inputs);
            for (HamakePath p : paths) {
                long stamp = getTimeStamp(p.getFileSystem(), p);
                if (stamp == 0) {
                	LOG.error("Some of input/dependency files not present!");
                    return -10;
                }
                if (stamp > mits)
                    mits = stamp;
            }
        }

        if (mits == -1 || mits > mots) {
            //check that input folder is not empty            
    		for(HamakePath input : inputs){
    			try{
    				if(ArrayUtils.isEmpty(input.getFileSystem().listStatus(input.getPathName()))){
    					LOG.warn("WARN: The input folder is empty for task " + getName());
    				}
    			}
    			catch(IOException e){
    				LOG.error(e);
    			}
    		}    		
    		if(inputs.isEmpty()){
    			LOG.warn("WARN: There is no input folder for task " + getName());
            	return 0;
            }
    		
            for (HamakePath input : inputs)
                input.removeIfExists(input.getFileSystem());

            return getTask().execute(context);
        }
        // all fresh
        return 0;
    }

    protected long getTimeStamp(FileSystem fs, HamakePath path) throws IOException {
        Path ipath = path.getPathName();
        synchronized (fs) {
            if (!fs.exists(ipath))
                return 0;
        }

        FileStatus stat;
        synchronized (fs) {
            stat = fs.getFileStatus(ipath);
        }

        if (path.hasFilename() || path.getMask() == null) {
            return stat.getModificationTime();
        }

        if (!stat.isDir()) {
            throw new IOException("Path " + ipath + " must be a dir!");
        }
        FileStatus list[];
        synchronized (fs) {
            list = fs.listStatus(ipath);
        }
        long ret = 0;
        for (FileStatus s : list) {
            if (Utils.matches(s.getPath(), path.getMask())) {
                if (s.getModificationTime() > ret) {
                    ret = s.getModificationTime();
                }
            }
        }
        return ret;
    }

}