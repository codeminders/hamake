package com.codeminders.hamake.tasks;

import com.codeminders.hamake.Path;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
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
        // TODO
        return -100;
    }

// TODO
    /*
        def execute(self, job_semaphore, exec_context):
            """ Execute command and return exit code
            """
            fsclient = exec_context['fsclient']
            if self.xinput.hasFilename():
                #TODO: file to file mapping
                return -100
            else:
                ipath = self.xinput.getHPathName()
                try:
                    inputlist = _getFileList(fsclient, ipath, False, self.xinput.mask)
                    if inputlist == None:
                        return -1
                except ThriftIOException, e:
                    print >> sys.stderr,"Error accessing %s" % self.xinput
                    return -1

                outputlists = []
                for output in self.outputs:
                    opath = output.getHPathName()
                    try:
                        outputlist = _getFileList(fsclient, opath, True)
                        if outputlist==None:
                            return -1
                        outputlists.append((output,outputlist))
                    except ThriftIOException, e:
                        print >> sys.stderr,"Error accessing %s" % output
                        return -1

                cmdparamsqueue = []
                for (iname,i) in inputlist.items():
                    iparams = [self.xinput.getPathWithNewName(iname)]
                    oparams = []
                    present = []
                    cleanuplist = []
                    for (output,outputlist) in outputlists:
                        oname = output.getPathName(iname)
                        if outputlist.has_key(iname):
                            if outputlist[iname].modification_time >= i.modification_time:
                                if hconfig.verbose:
                                    print >> sys.stderr, "Output %s is already present and fresh" % oname
                                present.append(oname)
                            else:
                                if hconfig.verbose:
                                    print >> sys.stderr, "Output %s is present but not fresh. Removing it." % oname
                                if not hconfig.dryrun:
                                    with fsclient.mutex:
                                        fsclient.rm(output.getHPathName(oname),True)
                        oparams.append(output.getPathWithNewName(iname))
                        cleanuplist.append(output.getHPathName(iname))
                    if len(present)==len(self.outputs):
                        if hconfig.verbose:
                            print >> sys.stderr, "All outputs of %s are fresh" % iname
                        continue # all files are fresh. no need to process this input

                    for pr in present:
                        print >> sys.stderr, "Removing partial output %s" % pr.pathname
                        if not hconfig.dryrun:
                            with fsclient.mutex:
                                fsclient.rm(pr,True)

                    params_dict = {PathParam.INPUTFILE_TYPE : iparams,
                                   PathParam.OUTPUTFILE_TYPE : oparams}

                    cmdparamsqueue.append((params_dict,cleanuplist))

                return self._execQueue(cmdparamsqueue, job_semaphore, exec_context)

        def _execQueue(self, cmdparamsqueue, job_semaphore, exec_context):
            threads = []
            for (cmdparams,cleanuplist) in cmdparamsqueue:
                job_semaphore.acquire()
                try:
                    t = CommandThread(self.command, cmdparams, cleanuplist, exec_context, job_semaphore)
                    threads.append(t)
                    t.start()
                except Exception:
                    print >> sys.stderr, "Unexpected exception starting thread!"
                    print >> sys.stderr, traceback.format_exc()
                    job_semaphore.release()

            rc = 0
            for t in threads:
                t.join()
                if t.rc!=0:
                    rc = t.rc
            return rc
            */

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