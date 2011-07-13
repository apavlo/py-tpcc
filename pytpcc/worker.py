#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo & Yang Lu
# http:##www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import sys
import os
import string
import datetime
import logging
import re
import argparse
import glob
import time 
import message
import pickle
import traceback
from pprint import pprint,pformat

from util import *
from runtime import *
import drivers

## ==============================================
## createDriverClass
## ==============================================
def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass
## DEF


## ==============================================
## loaderFunc
## ==============================================
def loaderFunc(driverClass, scaleParameters, args, config, w_ids, debug):
    driver = driverClass(args['ddl'])
    assert driver != None
    logging.debug("Starting client execution: %s [warehouses=%d]" % (driver, len(w_ids)))
    
    config['load'] = True
    config['execute'] = False
    config['reset'] = False
    driver.loadConfig(config)
   
    try:
        loadItems = (1 in w_ids)
        l = loader.Loader(driver, scaleParameters, w_ids, loadItems)
        driver.loadStart()
        l.execute()
        driver.loadFinish()   
    except KeyboardInterrupt:
            return -1
    except (Exception, AssertionError), ex:
        logging.warn("Failed to load data: %s" % (ex))
        #if debug:
        traceback.print_exc(file=sys.stdout)
        raise
        
## DEF


## ==============================================
## executorFunc
## ==============================================
def executorFunc(driverClass, scaleParameters, args, config, debug):
    driver = driverClass(args['ddl'])
    assert driver != None
    logging.debug("Starting client execution: %s" % driver)
    
    config['execute'] = True
    config['reset'] = False
    driver.loadConfig(config)

    e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
    driver.executeStart()
    results = e.execute(args['duration'])
    driver.executeFinish()
    
    return results
## DEF

## MAIN
if __name__=='__channelexec__':
    driverClass=None
    for item in channel:
       command=pickle.loads(item)
       if command.header==message.CMD_LOAD:
	   scaleParameters=command.data[0]
	   args=command.data[1]
	   config=command.data[2]
	   w_ids=command.data[3]
	   
	   ## Create a handle to the target client driver at the client side
           driverClass = createDriverClass(args['system'])
           assert driverClass != None, "Failed to find '%s' class" % args['system']
           driver = driverClass(args['ddl'])
           assert driver != None, "Failed to create '%s' driver" % args['system']
        
           loaderFunc(driverClass,scaleParameters,args,config,w_ids,True)
	   m=message.Message(header=message.LOAD_COMPLETED)
           channel.send(pickle.dumps(m,-1))          
       elif command.header==message.CMD_EXECUTE:
	   scaleParameters=command.data[0]
	   args=command.data[1]
	   config=command.data[2]
	  
	   ## Create a handle to the target client driver at the client side
	   if driverClass==None:
               driverClass = createDriverClass(args['system'])
               assert driverClass != None, "Failed to find '%s' class" % args['system']
               driver = driverClass(args['ddl'])
               assert driver != None, "Failed to create '%s' driver" % args['system']
           
           results=executorFunc(driverClass,scaleParameters,args,config,True)
           m=message.Message(header=message.EXECUTE_COMPLETED,data=results)
           channel.send(pickle.dumps(m,-1))
           
       elif command.header==message.CMD_STOP:
	   pass
       else:
	   pass
	 