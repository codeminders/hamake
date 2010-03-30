package com.codeminders.hamake.tasks;

import com.codeminders.hamake.CommandThread;
import com.codeminders.hamake.Config;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.Task;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.params.PathParam;
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

public class MapTask extends Task {

	public static final Log LOG = LogFactory.getLog(MapTask.class);
	
    private Collection<HamakePath> deps = new ArrayList<HamakePath>();
    private HamakePath xinput;

    public List<HamakePath> getInputs() {
        List<HamakePath> ret = new ArrayList<HamakePath>(getDeps());
        if (getXinput() != null) {
            ret.add(getXinput());
        }
        return ret;
    }

    public int execute(Semaphore semaphore, Map<String, Object> context) throws IOException {
        FileSystem fs = xinput.getFileSystem();
        if (xinput.hasFilename()) {
            // TODO: file 2 file mapping
            return -100;
        }

        Map<String, FileStatus> inputlist;

        try {
            inputlist = Utils.getFileList(xinput, false, xinput.getMask());
            if (inputlist == null)
                return -1;
            if(inputlist.isEmpty()){
            	LOG.warn("WARN: Input folder for task " + getName() + " is empty");
            	return 0;
            }
        } catch (IOException ex) {
        	LOG.error("Error accessing " + xinput, ex);
            return -1;
        }

        Collection<Object[]> outputlists = new ArrayList<Object[]>();
        for (HamakePath output : getOutputs()) {
            try {
                Map<String, FileStatus> outputlist = Utils.getFileList(output, true);
                if (outputlist == null)
                    return -1;
                outputlists.add(new Object[] {output, outputlist});
            } catch (IOException ex) {
            	LOG.error("Error accessing " + output, ex);
                return -1;
            }
        }

        Collection<Object[]> cmdparamsqueue = new ArrayList<Object[]>();
        boolean have_work = false;

        for (Map.Entry<String, FileStatus> entry : inputlist.entrySet()) {
            String iname = entry.getKey();
            FileStatus i = entry.getValue();
            Collection<HamakePath> iparams = new ArrayList<HamakePath>();
            iparams.add(xinput.getPathWithNewName(iname));
            Collection<HamakePath> oparams = new ArrayList<HamakePath>();
            Collection<Path> present = new ArrayList<Path>();
            Collection<Path> cleanuplist = new ArrayList<Path>();

            for (Object o[] : outputlists) {
                HamakePath output = (HamakePath) o[0];
                @SuppressWarnings("unchecked")
                Map<String, FileStatus> outputlist = (Map<String, FileStatus>) o[1];
                Path oname = output.getPathName();
                if (outputlist.containsKey(iname)) {
                    FileStatus stat = outputlist.get(iname);
                    if (stat.getModificationTime() >= i.getModificationTime()) {
                        if (Config.getInstance().verbose)
                        	LOG.info("Output " + oname + " is already present and fresh");                            
                        present.add(oname);
                    } else {
                        if (Config.getInstance().verbose)
                        	LOG.info("Output " + oname + " is present but not fresh. Removing it.");
                        if (!Config.getInstance().dryrun) {
                            synchronized (fs) {
                            	fs.delete(oname, true);
                            }
                        }
                    }
                }
                oparams.add(output.getPathWithNewName(iname));
                cleanuplist.add(output.getPathName());
            }
            if (present.size() == getOutputs().size()) {
                if (Config.getInstance().verbose)
                	LOG.info("All outputs of " + iname + " are fresh");
                // all files are fresh. no need to process this input
                continue;
            }

            have_work = true;

            for (Path pr : present) {
            	LOG.info("Removing partial output " + pr);
                if (!Config.getInstance().dryrun) {
                    synchronized (fs) {
                        fs.delete(pr, true);
                    }
                }
            }

            Map<String, Collection> param_dict = new HashMap<String, Collection>();
            param_dict.put(PathParam.Type.inputfile.name(), iparams);
            param_dict.put(PathParam.Type.outputfile.name(), oparams);
            cmdparamsqueue.add(new Object[]{param_dict, cleanuplist});


        }

        if (have_work)
            return execQueue(cmdparamsqueue, semaphore, context);
        else
            return 0;


    }

    protected int execQueue(Collection<Object[]> cmdparamsqueue,
                            Semaphore job_semaphore,
                            Map<String, Object> exec_context) {
        Collection<CommandThread> threads = new ArrayList<CommandThread>();
        for (Object o[] : cmdparamsqueue) {
            try {
                job_semaphore.acquire();
            } catch (InterruptedException ex) {
            	LOG.error(ex);
                return -1000;
            }
            try {
                @SuppressWarnings("unchecked")
                CommandThread t = new CommandThread(getCommand(),
                        (Map<String, Collection>) o[0],
                        (Collection<Path>) o[1],
                        exec_context,
                        job_semaphore);
                threads.add(t);
                t.start();
            } catch (Exception ex) {
            	LOG.error(ex);
                job_semaphore.release();

            }
        }
        int rc = 0;
        for (CommandThread t : threads) {
            try {
                t.join();
            } catch (InterruptedException ex) {
            	LOG.error(ex);
                return -1000;
            }
            int t_rc = t.getReturnCode();
            if (t_rc != 0)
                rc = t_rc;
        }
        return rc;

    }

    public Collection<HamakePath> getDeps() {
        return deps;
    }

    public void setDeps(Collection<HamakePath> deps) {
        this.deps = deps;
    }

    public HamakePath getXinput() {
        return xinput;
    }

    public void setXinput(HamakePath xinput) {
        this.xinput = xinput;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).
                append("inputs", getInputs()).
                append("deps", getDeps()).appendSuper(super.toString()).toString();
    }

}