"""
HAMake command execution related classes

@author Vadim Zaliva <lord@codeminders.com>
"""

from utils import *
from tasks import *

import hconfig

import threading, traceback, sys

class TaskThread(threading.Thread):
    
    def __init__(self, task, cv, job_semaphore, exec_context):
        threading.Thread.__init__(self, name="Task %s" % task.name)
        self.exec_context = exec_context
        self.job_semaphore = job_semaphore
        self.name = task.name
        self.task = task
        self.cv = cv
        self.finished = False
        self.rc = 0
        
    def run(self):
        try:
            rc = self.task.execute(self.job_semaphore, self.exec_context)
        except:
            print >> sys.stderr, 'Unexpected exception during task execution!'
            print >> sys.stderr, traceback.format_exc()
            rc = -1000
        self.cv.acquire()
        try:
            self.rc = rc
            self.finished = True
        
            self.cv.notify()
        finally:
            self.cv.release()

class TaskRunner:
    """ This class coordinates tasks lauching and execution """
    
    def __init__(self, taskslist, njobs, targets, exec_context):
        self.graph = DependencyGraph(taskslist, targets)
        self.exec_context = exec_context
        self.tasks={}
        for t in taskslist:
            self.tasks[t.name]=t
        self.completed = []
        self.failed = []
        self.running = []
        if njobs == -1:
            self.job_semaphore = FakeSemaphore()
        else:
            self.job_semaphore = threading.Semaphore(njobs)
        self.cv = threading.Condition()

    def startTasks(self, tasklist):
        for tn in tasklist:
            print >> sys.stderr, "Starting %s" % tn
            t = self.tasks[tn]
            ct = TaskThread(t, self.cv, self.job_semaphore, self.exec_context)
            self.running.append(ct)
            ct.start()

    def run(self):
        while True:
            self.cv.acquire()
            running_names = [x.name for x in self.running]
            candidates = [x for x in self.graph.getReadyToRunTasks() if x not in self.failed and x not in running_names]
            if len(candidates)==0 and len(self.running)==0:
                break

            self.startTasks(candidates)

            self.cv.wait()
            just_finished = [x for x in self.running if x.finished]
            for f in just_finished:
                if f.rc == 0:
                    print >> sys.stderr, "Execution of %s is completed" % (f.name)
                    self.completed.append(f.name)
                    self.graph.removeTask(f.name)
                else:
                    print >> sys.stderr, "Execution of %s is failed with code %d" % (f.name, f.rc)
                    self.failed.append(f.name)
            self.running = [x for x in self.running if not x.finished]
            self.cv.release()


class DependencyGraph:
    """ Class implementing simple algorithm to calculated tasks dependencies
    and order of execution """
    
    def __init__(self, tasklist, targets):
        self.tasks = {}
        self.addTasks(tasklist)
        if len(targets)>0:
            self.setTargets(targets)


    def setTargets(self, targets):
        """ Clean up dependency graph to build only given targets and tasks they depend on """
        if hconfig.test_mode:
            print >> sys.stderr, "Setting targets: %s" % targets
        while True:
            changed=False
            for (t,d) in self.tasks.items():
                if t in targets:
                    for di in d:
                        if not di in targets:
                            targets.append(di)
                            if hconfig.verbose:
                                print >> sys.stderr, "Adding task %s to build list since %s depends on it" % (di, t)
                            changed = True
            if not changed:
                break
        if hconfig.test_mode:
            print >> sys.stderr, "Final list of targets: %s" % targets
        newtasks = {}
        for (t,d) in self.tasks.items():
            if t in targets:
                newtasks[t]=d
        self.tasks=newtasks
    
    def addTasks(self, tasks):
        for t in tasks:
            d = []
            for ot in tasks:
                if t.dependsOn(ot):
                    d.append(ot.name)
            self.tasks[t.name]=d

    def removeTask(self, name):
        """ Remove task from the graph """
        del self.tasks[name]
        for (t,d) in self.tasks.items():
            self.tasks[t]=[x for x in d if x!=name]
        
    def getReadyToRunTasks(self):
        """ Returns tasks which could be executed """
        return [t for (t,d) in self.tasks.items() if len(d)==0]


    def dump(self):
        """ Debug method. dumps curent state to stdout """
        for (t,d) in self.tasks.items():
            print >> sys.stderr, "%s:%s" % (t, d)

