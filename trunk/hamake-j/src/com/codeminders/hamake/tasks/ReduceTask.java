package com.codeminders.hamake.tasks;

import com.codeminders.hamake.Path;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.params.PathParam;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSClient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;

public class ReduceTask extends BaseTask {

    private Collection<Path> deps = new ArrayList<Path>();
    private Collection<Path> inputs = new ArrayList<Path>();

    public Collection<Path> getInputs() {
        return new ArrayList<Path>(inputs);
    }

    public int execute(Semaphore semaphore, Map<String, Object> context) throws IOException {
        DFSClient fsclient = Utils.getFSClient(context);
        long mits = -1;
        long mots = -1;

        int numo = 0;
        for (Path p : getOutputs()) {
            long stamp = getTimeStamp(fsclient, p);
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
            Collection<Path> paths = new ArrayList<Path>(getInputs());
            paths.addAll(getDeps());
            for (Path p : paths) {
                long stamp = getTimeStamp(fsclient, p);
                if (stamp == 0) {
                    System.err.println("Some of input/dependency files not present!");
                    return -10;
                }
                if (stamp > mits)
                    mits = stamp;
            }
        }

        if (mits == -1 || mots == -1 || mits > mots) {
            Map<String, Collection> params = new HashMap<String, Collection>();
            params.put(PathParam.Type.input.name(), getInputs());
            params.put(PathParam.Type.dependency.name(), getDeps());
            params.put(PathParam.Type.output.name(), getOutputs());

            for (Path p : getOutputs())
                p.removeIfExists(fsclient);

            return getCommand().execute(params, context);
        }
        // all fresh
        return 0;
    }

    protected long getTimeStamp(DFSClient client, Path path) throws IOException {
        String ipath = path.getPathName();
        synchronized (client) {
            if (!client.exists(ipath))
                return 0;
        }

        FileStatus stat;
        synchronized (client) {
            stat = client.getFileInfo(ipath);
        }

        if (path.hasFilename() || path.getMask() == null) {
            return stat.getModificationTime();
        }

        if (!stat.isDir()) {
            throw new IOException("Path " + ipath + " must be a dir!");
        }
        FileStatus list[];
        synchronized (client) {
            list = client.listPaths(ipath);
        }
        long ret = 0;
        for (FileStatus s : list) {
            if (FilenameUtils.wildcardMatch(s.getPath().getName(), path.getMask())) {
                if (s.getModificationTime() > ret) {
                    ret = s.getModificationTime();
                }
            }
        }
        return ret;
    }

    public Collection<Path> getDeps() {
        return deps;
    }

    public void setDeps(Collection<Path> deps) {
        this.deps = deps;
    }

    public void setInputs(Collection<Path> inputs) {
        this.inputs = inputs;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).
                append("inputs", getInputs()).
                append("deps", getDeps()).appendSuper(super.toString()).toString();
    }

}