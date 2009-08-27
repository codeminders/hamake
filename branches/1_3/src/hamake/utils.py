"""
misc utiluty functions
"""

import time, os

class MagicDict(dict):
     def __getitem__(self, k):
         if k == 'timestamp':
             return str(time.time())
         else:
             return dict.__getitem__(self,k)
         
def getElementPath(dom):
    res = ""
    while dom:
        res = dom.nodeName+"/"
        dom = dom.parentNode
    return "/"+res
    
def getRequiredAttribute(dom, name, props=None):
    if dom.hasAttribute(name):
        res =  dom.getAttribute(name)
        if props:
            return res % props
        else:
            return res
    else:
        path = getElementPath(dom)
        raise Exception("Missing '%s' attribute in '%s' element" % (name, path))

def getOptionalAttribute(dom, name, props=None):
    if dom.hasAttribute(name):
        res = dom.getAttribute(name)
        if props:
            return res % props
        else:
            return res
    else:
        return None
    


class FakeSemaphore:
     """ Fake semaphore class. Never blocks. Always succeeds """
     
     def __init__(self, value=0):
          pass
     
     def acquire(self, blocking=False):
          pass

     def release(self):
          pass
     
                 
