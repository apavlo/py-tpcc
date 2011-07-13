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
import pickle
import execnet
import worker
import message
from ConfigParser import SafeConfigParser
from pprint import pprint,pformat

from util import *
from runtime import *
import drivers

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
                    
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
## getDrivers
## ==============================================
def getDrivers():
    drivers = [ ]
    for f in map(lambda x: os.path.basename(x).replace("driver.py", ""), glob.glob("./drivers/*driver.py")):
        if f != "abstract": drivers.append(f)
    return (drivers)
## DEF

## ==============================================
## startLoading
## ==============================================
def startLoading(scalParameters,args,config,channels):  
    #Split the warehouses into chunks
    procs = len(channels)
    w_ids = map(lambda x:[], range(procs))
    for w_id in range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1):
        idx = w_id % procs
        w_ids[idx].append(w_id)
    print w_ids
        
    load_start=time.time()
    for i in range(len(channels)):
        m=message.Message(header=message.CMD_LOAD,data=[scalParameters,args,config,w_ids[i]])
        channels[i].send(pickle.dumps(m,-1))
    for ch in channels:
        ch.receive()
        pass
    return time.time()-load_start


## ==============================================
## startExecution
## ==============================================
def startExecution(scaleParameters, args, config,channels):
    procs = len(channels)
    total_results = results.Results()
    
    for ch in channels:
        m=message.Message(header=message.CMD_EXECUTE,data=[scaleParameters,args,config])
        ch.send(pickle.dumps(m,-1))
    for ch in channels:
        r=pickle.loads(ch.receive()).data
        total_results.append(r)
    return (total_results)
## DEF


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark')
    aparser.add_argument('system', choices=getDrivers(),
                         help='Target system driver')
    aparser.add_argument('--config', type=file,
                         help='Path to driver configuration file')
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--warehouses', default=4, type=int, metavar='W',
                         help='Number of Warehouses')
    aparser.add_argument('--duration', default=60, type=int, metavar='D',
                         help='How long to run the benchmark in seconds')
    aparser.add_argument('--ddl', default=os.path.realpath(os.path.join(os.path.dirname(__file__), "tpcc.sql")),
                         help='Path to the TPC-C DDL SQL file')
    ## number of processes per node
    aparser.add_argument('--clientprocs', default=1, type=int, metavar='N',
                         help='Number of processes on each client node.')
                         
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file for the system and exit')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
        
    ## Create a handle to the target client driver
    driverClass = createDriverClass(args['system'])
    assert driverClass != None, "Failed to find '%s' class" % args['system']
    driver = driverClass(args['ddl'])
    assert driver != None, "Failed to create '%s' driver" % args['system']
    if args['print_config']:
        config = driver.makeDefaultConfig()
        print driver.formatConfig(config)
        print
        sys.exit(0)

    ## Load Configuration file
    if args['config']:
        logging.debug("Loading configuration file '%s'" % args['config'])
        cparser = SafeConfigParser()
        cparser.read(os.path.realpath(args['config'].name))
        config = dict(cparser.items(args['system']))
    else:
        logging.debug("Using default configuration for %s" % args['system'])
        defaultConfig = driver.makeDefaultConfig()
        config = dict(map(lambda x: (x, defaultConfig[x][1]), defaultConfig.keys()))
    config['reset'] = args['reset']
    config['load'] = False
    config['execute'] = False
    if config['reset']: logging.info("Reseting database")
    driver.loadConfig(config)
    logging.info("Initializing TPC-C benchmark using %s" % driver)
    
    
    ##Get a list of clientnodes from configuration file.
    clients=[]
    channels=[]
    assert config['clients']!=''
    clients=re.split(r"\s+",str(config['clients']))
    #print clients, len(clients),args['clientprocs']
    ##Create ssh channels to client nodes
    for node in clients:
        cmd = 'ssh='+ node
        cmd += r"//chdir="
        cmd += config['path']
        #print cmd
        for i in range(args['clientprocs']):
            gw=execnet.makegateway(cmd)
            ch=gw.remote_exec(worker)
            channels.append(ch)
    
    ## Create ScaleParameters
    scaleParameters = scaleparameters.makeWithScaleFactor(args['warehouses'], args['scalefactor'])
    nurand = rand.setNURand(nurand.makeForLoad())
    if args['debug']: logging.debug("Scale Parameters:\n%s" % scaleParameters)
    
    ## DATA LOADER!!!
    load_time = None
    if not args['no_load']:
        load_time = startLoading(scaleParameters, args, config,channels)
        #print load_time
    ## IF
    
    ## WORKLOAD DRIVER!!!
    if not args['no_execute']:
        results = startExecution(scaleParameters, args, config,channels)
        assert results
        print results.show(load_time)
    ## IF
    
## MAIN