package com.codeminders.hamake.tasks;

import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.Task;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.params.PathParam;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class ReduceTask extends Task {
	
	public static final Log LOG = LogFactory.getLog(ReduceTask.class);

    private Collection<HamakePath> inputs = new ArrayList<HamakePath>();

    public List<HamakePath> getInputs() {    	
        return new ArrayList<HamakePath>(inputs);
    }

    public int execute(Semaphore semaphore, Map<String, Object> context) throws IOException {
        long mits = -1;
        long mots = -1;                
        
        int numo = 0;
        for (HamakePath p : getOutputs()) {
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
            Collection<HamakePath> paths = new ArrayList<HamakePath>(getInputs());
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
            Map<String, List<HamakePath>> params = new HashMap<String, List<HamakePath>>();
            //check that input folder is not empty            
    		for(HamakePath path : inputs){
    			try{
    				if(ArrayUtils.isEmpty(path.getFileSystem().listStatus(path.getPathName()))){
    					LOG.warn("WARN: The input folder is empty for task " + getName());
    				}
    			}
    			catch(IOException e){
    				LOG.error(e);
    			}
    		}    		
    		if(getInputs().isEmpty()){
    			LOG.warn("WARN: There is no input folder for task " + getName());
            	return 0;
            }
    		
            params.put(PathParam.Type.input.name(), getInputs());
            params.put(PathParam.Type.output.name(), getOutputs());

            for (HamakePath p : getOutputs())
                p.removeIfExists(p.getFileSystem());

            return getCommand().execute(params, context);
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

    public void setInputs(List<HamakePath> inputs) {
        this.inputs = inputs;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).
                append("inputs", getInputs()).appendSuper(super.toString()).toString();
    }

}