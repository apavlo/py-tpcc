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
import getopt
import json
import csv
from pprint import pprint,pformat

from util import *
from runtime import *
import drivers

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
                    
OPT_DEBUG        = False
OPT_WAREHOUSES   = 10
OPT_SCALEFACTOR  = 1
OPT_DURATION     = 60
OPT_NO_LOAD      = False
OPT_NO_EXECUTE   = False
OPT_PRINT_CONFIG = False
OPT_DDL          = os.path.realpath(os.path.join(os.path.dirname(__file__), "tpcc.sql"))

## ==============================================
## create_handle
## ==============================================
def create_handle(name, ddl):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass(ddl)
## DEF

## ==============================================
## executeDriver
## ==============================================
def executeDriver(handle, duration, parameters):
    results = Results(handle)
    results.start()
    
    ## TODO: Do something!
    
    results.stop()
    return (results)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        ## Scale Factor
        "scalefactor=",
        ## Number of Warehouses
        "warehouses=",
        ## How long to run the benchmark
        "duration=",
        ## Disable loading the data
        "no-load",
        ## Disable executing the workload
        "no-execute",
        ## Print out the default configuration file for the system and exit
        "print-config",
        ## Enable debug logging
        "debug",
    ])
    ## ----------------------------------------------
    ## COMMAND OPTIONS
    ## ----------------------------------------------
    options = { }
    for key, value in _options:
       if key.startswith("--"): key = key[2:]
       if key in options:
          options[key].append(value)
       else:
          options[key] = [ value ]
    ## FOR
    ## Global Options
    for key in options:
        varname = "OPT_" + key.replace("-", "_").upper()
        if varname in globals() and len(options[key]) == 1:
            orig_val = globals()[varname]
            orig_type = type(orig_val) if orig_val != None else str
            if orig_type == bool:
                if not len(options[key][0]): options[key][0] = "true"
                val = (options[key][0].lower() == "true")
            else: 
                val = orig_type(options[key][0])
            globals()[varname] = val
            logging.debug("%s = %s" % (varname, str(globals()[varname])))
    ## FOR
    if OPT_DEBUG: 
        logging.getLogger().setLevel(logging.DEBUG)
    
    ## Create a handle to the client driver
    system_target = args[0]
    assert system_target, "Missing target system name"
    
    system_config = { "table_directory": "/tmp/tpcc-tables",
                      "txn_directory":   "/tmp/tpcc-txns",
                      "database": "/tmp/tpcc.db",
                      "reset":    False} # TODO
    #config_target = args[1]
    #assert config_target, "Missing target system configuration path"
    #with open(config_target, "r") as f:
        #json_config = json.load(f)

    ## Create the handle we'll need to use
    handle = create_handle(system_target, OPT_DDL)
    assert handle != None, "Failed to create '%s' handle" % system_target
    if OPT_PRINT_CONFIG:
        config = handle.makeDefaultConfig()
        print handle.formatConfig(config)
        sys.exit(0)
    
    handle.loadConfig(system_config)

    ## Create ScaleParameters
    parameters = scaleparameters.makeWithScaleFactor(OPT_WAREHOUSES, OPT_SCALEFACTOR)
    nurand = rand.setNURand(nurand.makeForLoad())
    
    ## DATA LOADER!!!
    if not OPT_NO_LOAD:
        handle.loadStart()
        loader.Loader(handle, parameters).execute()
        handle.loadFinish()
    
    ## WORKLOAD DRIVER!!!
    if not OPT_NO_EXECUTE:
        results = results.Results(handle)
        handle.executeStart()
        executor.Executor(handle, parameters).execute(results, OPT_DURATION)
        handle.executeFinish()
        print results
    ## IF
    
## MAIN