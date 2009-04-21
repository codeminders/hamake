"""
HAMake tasks classes

@author Vadim Zaliva <lord@crocodile.org>
"""

from __future__ import with_statement # 2.5 only

from utils import *
from time import sleep
import sys, subprocess, threading, string, traceback, os
from fnmatch import fnmatch
import xml.dom

import hconfig

from hadoopfs.ttypes import Pathname, ThriftIOException

# TODO: sort out negative return codes in this file
# and define constants for them


def _getFileList(fsclient, ipath, create = False, mask=None):
    """ Utility method to get list of files from DFS """
    if hconfig.test_mode:
        print >> sys.stderr, "Scanning %s" % ipath.pathname

    with fsclient.mutex:
        iexists = fsclient.exists(ipath)
    if not iexists:
        if create:
            print >> sys.stderr, "Creating %s" % ipath.pathname
            with fsclient.mutex:
                fsclient.mkdirs(ipath)
            return {}
        else:
            print >> sys.stderr, "path %s does not exists!" % ipath.pathname
            return None

    with fsclient.mutex:
        istat = fsclient.stat(ipath)
    if not istat.isdir:
        print >> sys.stderr, "path %s must be dir!" % ipath.pathname
        return None
    with fsclient.mutex:
        inputlist = fsclient.listStatus(ipath)
    res = {}
    for i in inputlist:
        pos = i.path.rfind('/')
        fname = i.path[pos+1:]
        if mask==None or fnmatch(fname, mask):
            res[fname] = i
    return res



class CommandThread(threading.Thread):
    
    def __init__(self, command, params, cleanuplist, exec_context, job_semaphore):
        threading.Thread.__init__(self, name=str(command))
        self.command = command
        self.params = params
        self.cleanuplist = cleanuplist
        self.exec_context = exec_context
        self.job_semaphore = job_semaphore

    def run(self):
        try:
            try:
                self.rc = self.command.execute(self.params, self.exec_context)
            except:
                print >> sys.stderr, "Execution of command %s failed" % self.command
                if hconfig.test_mode:
                    print >> sys.stderr, traceback.format_exc()
                self.rc = -1
            if self.rc!=0:
                self._cleanup()
        finally:
            self.job_semaphore.release()

    def _cleanup(self):
        fsclient = self.exec_context['fsclient']
        #TODO this would work only for files, not for paths with masks
        # use removeIfExists() instead
        for c in self.cleanuplist:
            with fsclient.mutex:
                cexists = fsclient.exists(c)
            if cexists:
                if hconfig.verbose:
                    print >> sys.stderr, "Removing %s" % c.pathname
                if not hconfig.dryrun:
                    with fsclient.mutex:
                        fsclient.rm(c, True)

class BaseCommand:
    
    def __init__(self):
        pass

    @classmethod
    def paramsFromDOM(cls, dom, props):
        scriptparam = []
        pindex=0
        for c in dom.childNodes:
            if c.nodeType == xml.dom.Node.ELEMENT_NODE:
                if c.nodeName == "constparam":
                    p = ConstParam.fromDOM(c, props)
                elif c.nodeName == "pathparam":
                    p = PathParam.fromDOM(c, props)
                elif c.nodeName == "jobconfparam":
                    p = JobConfParam.fromDOM(c, props)
                elif c.nodeName == "pigparam":
                    p = PigParam.fromDOM(c, props)
                else:
                    raise Exception("Unknown sub-element '%s' under %s" % (c.nodeName, getElementPath(c)))
                scriptparam.append(p)
        return scriptparam
        

class Param:
    pass

class ConstParam(Param):
    def __init__(self, value):
        self.value = value

    def get(self, param_dict, fsclient = None):
        return [self.value]
   
    @classmethod
    def fromDOM(cls, dom, props):
        value = getRequiredAttribute(dom, "value", props)
        return ConstParam(value)

class JobConfParam(Param):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def get(self, param_dict, fsclient = None):
        return ["-jobconf", "%s=%s" % (self.name, self.value)]
   
    @classmethod
    def fromDOM(cls, dom, props):
        name = getRequiredAttribute(dom, "name", props)
        value = getRequiredAttribute(dom, "value", props)
        return JobConfParam(name, value)

class PigParam(Param):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def get(self, param_dict, fsclient = None):
        return ["-param", "%s=%s" % (self.name, self.value)]
   
    @classmethod
    def fromDOM(cls, dom, props):
        name = getRequiredAttribute(dom, "name", props)
        value = getRequiredAttribute(dom, "value", props)
        return PigParam(name, value)

class PathParam(Param):

    # 'Type' attribute values
    INPUT_TYPE      = "input"
    OUTPUT_TYPE     = "output"
    DEP_TYPE        = "dependency"
    INPUTFILE_TYPE  = "inputfile"
    OUTPUTFILE_TYPE = "outputfile"

    # 'Mask' attribute values
    KEEP_MASK    = "keep"
    SUPRESS_MASK = "suppress"
    EXPAND_MASK  = "expand"
    
    def __init__(self, ptype, number, mask_handling=KEEP_MASK):
        self.ptype = ptype
        self.number = number
        self.mask_handling = mask_handling


    def _toStrArr(self, i, fsclient = None):
        if isinstance(i, Path):
            if self.mask_handling == PathParam.KEEP_MASK:
                return [i.getPathNameWithMask()]
            elif self.mask_handling == PathParam.SUPRESS_MASK:
                return [i.getPathName()]
            elif self.mask_handling == PathParam.EXPAND_MASK:
                if fsclient == None:
                    raise Exception("Could not expand path, no fsclient")
                if i.filename != None:
                    raise Exception("Could not expand file %s" % i)
                el = _getFileList(fsclient, i.getHPathName(), False, mask=i.mask)
                return [i.getPathName(eli) for eli in el.keys()]
            else:
                raise Exception("Unexpected mask handling value")
        else:
            return [str(i)]
        

    def get(self, param_dict, fsclient = None):
        if self.number==-1:
            # mitliple inputs, may all be expanded. flatten results
            res = []
            for i in param_dict[self.ptype]:
                res = res + self._toStrArr(i,fsclient)
            return res
        else:
            return self._toStrArr(param_dict[self.ptype][self.number], fsclient)

    @classmethod
    def fromDOM(cls, dom, props):
        ptype = getRequiredAttribute(dom, "type", props)
        number_s = getOptionalAttribute(dom, "number", props)
        if number_s!=None:
            number = int(number_s)
        else:
            number = -1
        mask_handling = getOptionalAttribute(dom, "mask", props)
        if mask_handling == None:
            mask_handling = PathParam.KEEP_MASK
        else:
            if mask_handling not in [PathParam.KEEP_MASK, PathParam.SUPRESS_MASK, PathParam.EXPAND_MASK]:
                raise Exception("Unsupported value of 'mask' parameter in %s" % getElementPath(dom))
        return PathParam(ptype, number, mask_handling)

    
class PigCommand(BaseCommand):

    """ Default name of pig executable """
    PIGCMD="pig"

    """ Name of env. var. which holds name of pig executable """
    PIGCMDENV="PIG"
    
    def __init__(self, script, scriptparam=[]):
        BaseCommand.__init__(self)
        self.script = script
        self.scriptparam = scriptparam

    def execute(self, params_dict, exec_context):
        cmd = [os.environ.get(PigCommand.PIGCMDENV, PigCommand.PIGCMD)]
        fsclient = exec_context['fsclient']
        for p in self.scriptparam:
            cmd = cmd + p.get(params_dict, fsclient)
        cmd.append("-f")
        cmd.append(self.script)
        scmd = string.join(cmd)
        if hconfig.verbose:
            print >> sys.stderr, "Executing: %s" % scmd
        try:
            if hconfig.dryrun:
                return 0
            else:
                return subprocess.call(cmd)
        except:
            print >> sys.stderr, '%s execution failed!' % scmd
            if hconfig.test_mode:
                print >> sys.stderr, traceback.format_exc()
            return -1000
    

    @classmethod
    def fromDOM(cls, dom, props):
        script = getRequiredAttribute(dom, "script", props)
        scriptparam = BaseCommand.paramsFromDOM(dom, props) 
        return PigCommand(script, scriptparam)

class HadoopCommand(BaseCommand):

    """ Default name of hadoop executable """
    HADOOPCMD="hadoop"

    """ Name of env. var. which holds name of hadoop executable """
    HADOOPCMDENV="HADOOP"

    def __init__(self, jar, main, scriptparam):
        BaseCommand.__init__(self)
        self.jar = jar
        self.main = main
        self.scriptparam = scriptparam

    def execute(self, params_dict, exec_context):
        fsclient = exec_context['fsclient']
        cmd = [os.environ.get(HadoopCommand.HADOOPCMDENV, HadoopCommand.HADOOPCMD), "jar", self.jar, self.main]
        # first add jobconf params
        for p in self.scriptparam:
            if isinstance(p, JobConfParam):
                cmd = cmd + p.get(params_dict, fsclient)
        # then regular arguments, passed to main
        for p in self.scriptparam:
            if not isinstance(p, JobConfParam):
                cmd = cmd + p.get(params_dict, fsclient)
        scmd = string.join(cmd)
        if hconfig.verbose:
            print >> sys.stderr, "Executing: %s" % scmd
        try:
            if hconfig.dryrun:
                return 0
            else:
                return subprocess.call(cmd)
        except:
            print >> sys.stderr, '%s execution failed!' % scmd
            if hconfig.test_mode:
                print >> sys.stderr, traceback.format_exc()
            return -1000

    @classmethod
    def fromDOM(cls, dom, props):
        jar = getRequiredAttribute(dom, "jar", props)
        main = getRequiredAttribute(dom, "main", props)
        scriptparam = BaseCommand.paramsFromDOM(dom, props) 
        return HadoopCommand(jar, main, scriptparam)

        
class Path:
    
    def __init__(self, loc, filename=None, mask=None, gen=0):
        """ Private constructor, should never be called directly """
        self.loc = loc
        if filename and mask:
            raise Exception("Both filename and mask specified!")
        self.filename = filename
        self.mask = mask
        self.gen = gen

    def __str__(self):
        res = self.loc+"/"
        if self.filename:
            res = res + self.filename
        if self.mask:
            res = res + self.mask
        if self.gen>0:
            res = res + "@" + str(self.gen)
        return res

    def intersects(self, other):
        return self.loc == other.loc and (self.filename == other.filename or
                                          other.filename == None or
                                          self.filename == None) and self.gen>=other.gen
    

    def getHPathName(self, new_filename=None):
        return Pathname({'pathname': self.getPathName(new_filename)})

    def getPathWithNewName(self, new_filename):
        if new_filename:
            new_mask = None
        else:
            new_mask = self.mask
        return Path(self.loc, new_filename, new_mask, self.gen)

    def getPathName(self, new_filename=None):
        if new_filename==None:
            new_filename = self.filename
        if new_filename!=None:
            p = self.loc + "/"+new_filename
        else:
            p = self.loc
        return p

    def getPathNameWithMask(self, new_filename=None):
        p = self.getPathName(new_filename)
        if self.mask:
            return p+"/"+self.mask
        else:
            return p
                 
    def hasFilename(self):
        return self.filename!=None

    def removeIfExists(self, fsclient):
        ipath = self.getHPathName()
        if self.filename!=None or self.mask==None:
            with fsclient.mutex:
                iexists = fsclient.exists(ipath)
            if iexists:
                if hconfig.verbose:
                    print >> sys.stderr, "Removing %s" % ipath.pathname
                if not hconfig.dryrun:
                    with fsclient.mutex:
                        fsclient.rm(ipath,True)
        else:
            with fsclient.mutex:
                istat = fsclient.stat(ipath)
            if not istat.isdir:
                raise Exception("path %s must be dir!" % ipath.pathname)
            with fsclient.mutex:
                inputlist = fsclient.listStatus(ipath)
            for i in inputlist:
                pos = i.path.rfind('/')
                fname = i.path[pos+1:]
                if fnmatch(fname, self.mask):
                    fpath = self.getHPathName(fname)
                    with fsclient.mutex:
                        fexists = fsclient.exists(fpath)
                    if fexists:
                        if hconfig.verbose:
                            print >> sys.stderr, "Removing %s" % fpath.pathname
                        if not hconfig.dryrun:
                            with fsclient.mutex:
                                fsclient.rm(fpath,True)

    @classmethod
    def fromDOM(cls, dom, props):
        loc = getRequiredAttribute(dom, 'location', props)
        filename = getOptionalAttribute(dom, 'filename', props)
        mask = getOptionalAttribute(dom, 'mask', props)
        gen_s = getOptionalAttribute(dom, 'generation', props)
        if gen_s == None:
            gen = 0
        else:
            gen = int(gen_s)
        return Path(loc, filename, mask, gen)
    
class BaseTask:
    """ Base class for all tasks """

    def __init__(self):
        self.name = None
        self.outputs = []
        self.command = None

    def getName(self):
        return name
    
    def getInputs(self):
        return None

    def getOutputs(self):
        return self.outputs

    def dependsOn(self, other):
        for i in self.getInputs():
            for o in other.getOutputs():
                if i.intersects(o):
                    return True
        return False

    @classmethod
    def _parseCommands(cls, dom, props):
        i = dom.getElementsByTagName('task')
        if len(i)>1:
            path = getElementPath(dom)
            raise Exception("Multiple elements 'task' in %s are not permitted" % (name, path))
        if len(i)==1:
            return HadoopCommand.fromDOM(i[0], props)

        i = dom.getElementsByTagName('pig')
        if len(i)>1:
            path = getElementPath(dom)
            raise Exception("Multiple elements 'pig' in %s are not permitted" % (name, path))
        if len(i)==1:
            return PigCommand.fromDOM(i[0], props)

    @classmethod
    def _parsePathList(cls, dom, name, props):
        i = dom.getElementsByTagName(name)
        if not i or len(i)==0:
            return []
        elif len(i)!=1:
            path = getElementPath(dom)
            raise Exception("Multiple elements '%s' in %s are not permitted" % (name, path))
        else:
            px = i[0].getElementsByTagName("path")
            if not px:
                return []
            else:
                return [Path.fromDOM(p,props) for p in px]


class MapTask(BaseTask):
    """ map:: Input->[Depenency]->[Outputs] """

    def __init__(self):
        BaseTask.__init__(self)
        self.xinput = None
        self.deps = []
        
    def getInputs(self):
        if self.xinput:
            return self.deps + [self.xinput]
        else:
            return self.deps +[]
        
    @classmethod
    def fromDOM(cls, dom, props):
        name = getRequiredAttribute(dom, "name", props)

        inputs = BaseTask._parsePathList(dom, 'input', props)
        if len(inputs)==0:
            xinput = None
        elif len(inputs)==1:
            xinput = inputs[0]
        else:
            raise Exception("Multiple 'input' elements in MAP task '%s' are not permitted" % name)

        outputs = BaseTask._parsePathList(dom, 'output', props)
        deps = BaseTask._parsePathList(dom, 'dependencies', props)
        command = BaseTask._parseCommands(dom, props)


        # Sanity checks
        itype = xinput.hasFilename()
        for o in outputs:
            if o.hasFilename() != itype:
                raise Exception("Input/Output type/file mismatch for MAP task")
        
        res = MapTask()
        res.name = name 
        res.xinput = xinput
        res.outputs = outputs
        res.deps = deps
        res.command = command

        return res

    def execute(self, job_semaphore, exec_context):
        """ Execute command and return exit code """
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
                            with fsclient.mutex:
                                fsclient.rm(oname,True)
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
            except:
                print >> sys.stderr, "Unexpected exception starting thread!"
                print >> sys.stderr, traceback.format_exc()
                job_semaphore.release()

        rc = 0
        for t in threads:
            t.join()
            if t.rc!=0:
                rc = t.rc
        return rc

class ReduceTask(BaseTask):
    """ map:: [Input]->[Outputs] """
    
    def __init__(self):
        BaseTask.__init__(self)
        self.inputs = []
    
    def getInputs(self):
        return self.inputs

    @classmethod
    def fromDOM(cls, dom, props):
        name = getRequiredAttribute(dom, "name", props)

        inputs = BaseTask._parsePathList(dom, 'input', props)
        outputs = BaseTask._parsePathList(dom, 'output', props)
        deps = BaseTask._parsePathList(dom, 'dependencies', props)
        command = BaseTask._parseCommands(dom, props)

        res = ReduceTask()
        res.name = name 
        res.inputs = inputs
        res.outputs = outputs
        res.deps = deps
        res.command = command

        return res

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
        """ Execute command and return exit code """
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
