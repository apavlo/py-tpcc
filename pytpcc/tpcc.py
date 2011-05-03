#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
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

##
## Usage:
## python ./main.py [system name] [path/to/config.file]

import sys
import os
import string
import datetime
import logging
import re
import argparse
import glob
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
## createDriverHandle
## ==============================================
def createDriverHandle(name, ddl):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass(ddl)
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
        
    ## Create a handle to the client driver
    handle = createDriverHandle(args['system'], args['ddl'])
    assert handle != None, "Failed to create '%s' handle" % args['system']
    if args['print_config']:
        config = handle.makeDefaultConfig()
        print handle.formatConfig(config)
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
        defaultConfig = handle.makeDefaultConfig()
        config = dict(map(lambda x: (x, defaultConfig[x][1]), defaultConfig.keys()))
    config['reset'] = args['reset']
    handle.loadConfig(config)
    logging.info("Initializing TPC-C benchmark using %s" % handle)

    ## Create ScaleParameters
    parameters = scaleparameters.makeWithScaleFactor(args['warehouses'], args['scalefactor'])
    nurand = rand.setNURand(nurand.makeForLoad())
    if args['debug']: logging.debug("Scale Parameters:\n%s" % parameters)
    
    ## DATA LOADER!!!
    if not args['no_load']:
        handle.loadStart()
        loader.Loader(handle, parameters).execute()
        handle.loadFinish()
    
    ## WORKLOAD DRIVER!!!
    if not args['no_execute']:
        results = results.Results(handle)
        handle.executeStart()
        executor.Executor(handle, parameters).execute(results, args['duration'])
        handle.executeFinish()
        print results
    ## IF
    
## MAIN