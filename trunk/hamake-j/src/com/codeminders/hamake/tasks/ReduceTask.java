package com.codeminders.hamake.tasks;

import com.codeminders.hamake.Path;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class ReduceTask extends BaseTask {

    private Collection<Path> deps = new ArrayList<Path>();
    private Collection<Path> inputs = new ArrayList<Path>();

    public Collection<Path> getInputs() {
        return new ArrayList<Path>(inputs);
    }

    public int execute(Object semaphore, Map<String, Object> context) {
        // TODO
        return -100;
    }


    // TODO
    /*
    def getTimeStamp(self, fsclient, path):
        ipath = path.getHPathName()
        with fsclient.mutex:
            if not fsclient.exists(ipath):
                return 0
        if path.hasFilename() or path.mask == None:
            with fsclient.mutex:
                istat = fsclient.stat(ipath)
            return istat.modification_time
        else:
            with fsclient.mutex:
                istat = fsclient.stat(ipath)
            if not istat.isdir:
                raise Exception("path %s must be dir!" % ipath.pathname)
            with fsclient.mutex:
                inputlist = fsclient.listStatus(ipath)
            res = 0
            for i in inputlist:
                pos = i.path.rfind('/')
                fname = i.path[pos+1:]
                if fnmatch(fname, path.mask):
                    fpath = path.getHPathName(fname)
                    with fsclient.mutex:
                        istat = fsclient.stat(fpath)
                    if istat.modification_time>res:
                        res = istat.modification_time
            return res



    def execute(self, job_semaphore, exec_context):
        """ Execute command and return exit code
        """
        fsclient = exec_context['fsclient']
        mits = 0
        mots = 0
        ots = [self.getTimeStamp(fsclient, d) for d in self.outputs]
        if len(ots)>0:
            if 0 in ots:
                mots = -1
            else:
                mots = max(ots)
                # got output timestamp, check inputs
                deps = self.inputs + self.deps
                its = [self.getTimeStamp(fsclient, d) for d in deps]
                if len(its)>0:
                    if 0 in its:
                        print >> sys.stderr, "Some of input/dependency files not present!"
                        return -10
                    mits = max(its)
                else:
                    mits = -1 # no inputs, always run
        else:
            mots = -1 # no outputs, running for side-effects


        if mits==-1 or mots==-1 or mits>mots:
            params_dict = {PathParam.INPUT_TYPE : self.inputs,
                           PathParam.DEP_TYPE : self.deps,
                           PathParam.OUTPUT_TYPE : self.outputs}

            for o in self.outputs:
                o.removeIfExists(fsclient)

            return self.command.execute(params_dict, exec_context)
        else:
            return 0 # all fresh
            */

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