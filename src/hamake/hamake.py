"""
Hadoop Make

@author Vadim Zaliva <lord@crocodile.org>
"""

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

import hadoopfs.ThriftHadoopFileSystem
from hadoopfs.ttypes import *

import os, sys, getopt, traceback, threading
from os.path import basename
from xml.dom.minidom import parse, parseString

from tasks import *
from execution import *
from utils import *

import exitcodes
import hconfig

DEFAULT_MAKEFILE_NAME = 'hamakefile.xml'

def usage():
    print "Usage: hamake.py [--dry-run] [-j N] [--verbose] [--test] [-f hamakefile.xml] [<target> ...]"


class HMake:
    def __init__(self, fname, njobs, targets):
        if not os.path.exists(fname):
            raise Exception("HAMakefile not found: %s" % fname)
        self.tasks = self.loadHaMakefile(fname)
        self.njobs = njobs
        self.targets = targets
        

    def run(self):
        self.connectToDFS()
        exec_context = {'fsclient':self.fsclient}
        try:            
            runner = TaskRunner(self.tasks, self.njobs, self.targets, exec_context)
            runner.run()
            if len(runner.failed)>0:
                return exitcodes.FAILED
            else:
                return exitcodes.OK
        finally:
            self.transport.close()
            
    def connectToDFS(self):
        self.socket = TSocket.TSocket(self.thrift_host, self.thrift_port)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.fsclient = hadoopfs.ThriftHadoopFileSystem.Client(self.protocol)
        self.fsclient.mutex = threading.Lock()
        self.transport.open()
        
    def loadHaMakefile(self,fname):
        f = open(fname)
        dom = parse(f)
        props = self.parseProperties(dom) # only used on loading stage
        self.parseConfig(dom, props)
        tasks = self.parseTasks(dom, props)
        dom.unlink()
        f.close()
        if hconfig.test_mode:
            print >> sys.stderr, "%d tasks loaded" % len(tasks)
        return tasks
        
    def parseProperties(self, dom):
        c = dom.getElementsByTagName("property")
        res = MagicDict()
        if c:
            for px in c:
                pname = getRequiredAttribute(px,'name')
                pval = getRequiredAttribute(px,'value')
                res[pname] = pval
        return res
    
    def parseConfig(self, dom, props):
        c = dom.getElementsByTagName("config")
        if not c or len(c)!=1:
            raise Exception("Missing or ambiguous 'config' section")
        d=c[0].getElementsByTagName("dfs")
        if not d or len(d)!=1:
            raise Exception("Missing or ambiguous 'config/dfs' section")
        
        n=d[0].getElementsByTagName("thriftAPI")
        if not n or len(n)!=1:
            raise Exception("Missing or ambiguous 'config/dfs/thriftAPI' element")
        self.thrift_host = getRequiredAttribute(n[0],'host', props)
        self.thrift_port = int(getRequiredAttribute(n[0],'port', props))
        
    def parseTasks(self, dom, props):
        tasks = []
        tx = dom.getElementsByTagName("map")
        if tx:
            for t in tx:
                task = MapTask.fromDOM(t,props)
                tasks.append(task)
        tx = dom.getElementsByTagName("reduce")
        if tx:
            for t in tx:
                task = ReduceTask.fromDOM(t,props)
                tasks.append(task)
        return tasks
                

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "Vvdtj:f:", ["version", "verbose", "dry-run", "test", "jobs","file"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print >> sys.stderr, str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(exitcodes.BADOPT)

    njobs = -1
    mname = DEFAULT_MAKEFILE_NAME;
    
    for o, a in opts:
        if o in ("-V", "--version"):
            print "HAMake version %s" % hconfig.version
            sys.exit(0)
        if o in ("-v", "--verbose"):
            hconfig.verbose = True
        elif o in ("-d", "--dry-run"):
            hconfig.dryrun = True
        elif o in ("-t", "--test"):
            hconfig.test_mode = True
        elif o in ("-j", "--jobs"):
            njobs = int(a)
        elif o in ("-f", "--file"):
            mname = a
        else:
            assert False, "unhandled option %s" % o

    try:
        m = HMake(mname, njobs, args)
    except:
        print >> sys.stderr, 'Error initializing hamake process'
        xvalue = sys.exc_info()[1]
        if xvalue!=None and hasattr(xvalue, 'message'):
            print >> sys.stderr, xvalue.message
        if hconfig.test_mode:
            traceback.print_exc(file=sys.stderr)
        sys.exit(exitcodes.INITERR)

    try:
        rc=m.run()
    except:
        print >> sys.stderr, 'HMake failed'
        xvalue = sys.exc_info()[1]
        if xvalue!=None and hasattr(xvalue, 'message'):
            print >> sys.stderr, xvalue.message
        if hconfig.test_mode:
            traceback.print_exc(file=sys.stderr)
        sys.exit(exitcodes.FAILED)
    else:
        sys.exit(rc)
