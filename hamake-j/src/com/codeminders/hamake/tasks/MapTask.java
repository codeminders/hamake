package com.codeminders.hamake.tasks;

import com.codeminders.hamake.Path;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.Config;
import com.codeminders.hamake.CommandThread;
import com.codeminders.hamake.params.PathParam;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

public class MapTask extends BaseTask {

    private Collection<Path> deps = new ArrayList<Path>();
    private Path xinput;

    public Collection<Path> getInputs() {
        Collection<Path> ret = new ArrayList<Path>(getDeps());
        if (getXinput() != null) {
            ret.add(getXinput());
        }
        return ret;
    }

    public int execute(Semaphore semaphore, Map<String, Object> context) throws IOException {
        DFSClient fsclient = Utils.getFSClient(context);
        if (xinput.hasFilename()) {
            // TODO: file 2 file mapping
            return -100;
        }

        Map<String, FileStatus> inputlist;

        String ipath = xinput.getPathName();
        try {
            inputlist =
                    Utils.getFileList(fsclient, ipath, false, xinput.getMask());
            if (inputlist == null)
                return -1;
        } catch (IOException ex) {
            System.err.println("Error accessing " + xinput + ": " + ex.getMessage());
            if (Config.getInstance().test_mode)
                ex.printStackTrace();
            return -1;
        }

        Collection<Object[]> outputlists = new ArrayList<Object[]>();
        for (Path output : getOutputs()) {
            String opath = output.getPathName();
            try {
                Map<String, FileStatus> outputlist = Utils.getFileList(fsclient, opath, true);
                if (outputlist == null)
                    return -1;
                outputlists.add(new Object[] {output, outputlist});
            } catch (IOException ex) {
                System.err.println("Error accessing " + output + ": " + ex.getMessage());
                if (Config.getInstance().test_mode)
                    ex.printStackTrace();
                return -1;
            }
        }

        Collection<Object[]> cmdparamsqueue = new ArrayList<Object[]>();
        boolean have_work = false;

        for (Map.Entry<String, FileStatus> entry : inputlist.entrySet()) {
            String iname = entry.getKey();
            FileStatus i = entry.getValue();
            Collection<Path> iparams = new ArrayList<Path>();
            iparams.add(xinput.getPathWithNewName(iname));
            Collection<Path> oparams = new ArrayList<Path>();
            Collection<String> present = new ArrayList<String>();
            Collection<String> cleanuplist = new ArrayList<String>();

            for (Object o[] : outputlists) {
                Path output = (Path) o[0];
                @SuppressWarnings("unchecked")
                Map<String, FileStatus> outputlist = (Map<String, FileStatus>) o[1];
                String oname = output.getPathName(iname);
                if (outputlist.containsKey(iname)) {
                    FileStatus fs = outputlist.get(iname);
                    if (fs.getModificationTime() >= i.getModificationTime()) {
                        if (Config.getInstance().verbose)
                            System.err.println("Output " + oname + " is already present and fresh");
                        present.add(oname);
                    } else {
                        if (Config.getInstance().verbose)
                            System.err.println("Output " + oname + " is present but not fresh. Removing it.");
                        if (!Config.getInstance().dryrun) {
                            synchronized (fsclient) {
                                fsclient.delete(output.getPathName(oname), true);
                            }
                        }
                    }
                }
                oparams.add(output.getPathWithNewName(iname));
                cleanuplist.add(output.getPathName(iname));
            }
            if (present.size() == getOutputs().size()) {
                if (Config.getInstance().verbose)
                    System.err.println("All outputs of " + iname + " are fresh");
                // all files are fresh. no need to process this input
                continue;
            }

            have_work = true;

            for (String pr : present) {
                System.err.println("Removing partial output " + pr);
                if (!Config.getInstance().dryrun) {
                    synchronized (fsclient) {
                        fsclient.delete(pr, true);
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
                System.err.println("Execution is interrupted");
                return -1000;
            }
            try {
                @SuppressWarnings("unchecked")
                CommandThread t = new CommandThread(getCommand(),
                        (Map<String, Collection>) o[0],
                        (Collection<String>) o[1],
                        exec_context,
                        job_semaphore);
                threads.add(t);
                t.start();
            } catch (Exception ex) {
                System.err.println("Unexpected exception starting thread!");
                ex.printStackTrace();
                job_semaphore.release();

            }
        }
        int rc = 0;
        for (CommandThread t : threads) {
            try {
                t.join();
            } catch (InterruptedException ex) {
                System.err.println("Execution is interrupted");
                return -1000;
            }
            int t_rc = t.getReturnCode();
            if (t_rc != 0)
                rc = t_rc;
        }
        return rc;

    }

    public Collection<Path> getDeps() {
        return deps;
    }

    public void setDeps(Collection<Path> deps) {
        this.deps = deps;
    }

    public Path getXinput() {
        return xinput;
    }

    public void setXinput(Path xinput) {
        this.xinput = xinput;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).
                append("inputs", getInputs()).
                append("deps", getDeps()).appendSuper(super.toString()).toString();
    }

}