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
## python ./main.py [system name] [path/to/config.json]

import sys
import os
import string
import datetime
import logging
import re
import getopt
import json
import csv
from random import shuffle
from pprint import pprint,pformat

import drivers
import scaleparameters
import nurand
import rand
import generator
from constants import *

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
                    
OPT_DEBUG       = False
OPT_WAREHOUSES  = 10
OPT_SCALEFACTOR = 1
OPT_DURATION    = 60

## ==============================================
## create_handle
## ==============================================
def create_handle(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass()
## DEF

## ==============================================
## executeLoader
## ==============================================
def executeLoader(handle, parameters, batch_size = 100):
    
    ## Select 10% of the rows to be marked "original"
    originalRows = rand.selectUniqueIds(parameters.items / 10, 1, parameters.items)
    
    ## Load all of the items
    tuples = [ ]
    for i in range(1, parameters.items+1):
        original = (i in originalRows)
        tuples.append(generator.generateItem(i, original))
        if len(tuples) == batch_size:
            logging.debug("%s: %d / %d" % (TABLENAME_ITEM, len(tuples), parameters.items))
            handle.loadTuples(TABLENAME_ITEM, tuples)
            tuples = [ ]
    ## FOR
    if len(tuples) > 0:
        logging.debug("%s: %d / %d" % (TABLENAME_ITEM, len(tuples), parameters.items))
        handle.loadTuples(TABLENAME_ITEM, tuples)
        
    ## Then create the warehouse-specific tuples
    for w_id in range(parameters.starting_warehouse, parameters.max_w_id+1):
        loadWarehouse(handle, w_id, parameters)
    ## FOR
    
    return (None)

## ==============================================
## loadWarehouse
## ==============================================
def loadWarehouse(handle, w_id, parameters):
    ## WAREHOUSE
    w_tuples = [ generator.generateWarehouse(w_id) ]
    handle.loadTuples(TABLENAME_WAREHOUSE, w_tuples)

    ## DISTRICT
    d_tuples = [ ]
    for d_id in range(1, parameters.districtsPerWarehouse+1):
        d_next_o_id = parameters.customersPerDistrict + 1
        d_tuples.append(generator.generateDistrict(w_id, d_id, d_next_o_id))
        
        c_tuples = [ ]
        h_tuples = [ ]
        
        ## Select 10% of the customers to have bad credit
        selectedRows = rand.selectUniqueIds(parameters.customersPerDistrict / 10, 1, parameters.customersPerDistrict)
        
        ## TPC-C 4.3.3.1. says that o_c_id should be a permutation of [1, 3000]. But since it
        ## is a c_id field, it seems to make sense to have it be a permutation of the
        ## customers. For the "real" thing this will be equivalent
        cIdPermutation = [ ]

        for c_id in range(1, parameters.customersPerDistrict+1):
            badCredit = (c_id in selectedRows)
            c_tuples.append(generator.generateCustomer(w_id, d_id, c_id, badCredit, True))
            d_tuples.append(generator.generateHistory(w_id, d_id, c_id))
            cIdPermutation.append(c_id)
        ## FOR
        assert cIdPermutation[0] == 1
        assert cIdPermutation[parameters.customersPerDistrict - 1] == parameters.customersPerDistrict
        shuffle(cIdPermutation)
        
        o_tuples = [ ]
        ol_tuples = [ ]
        no_tuples = [ ]
        
        for o_id in range(1, parameters.customersPerDistrict):
            o_ol_cnt = rand.number(MIN_OL_CNT, MAX_OL_CNT)
            
            ## The last newOrdersPerDistrict are new orders
            newOrder = ((parameters.customersPerDistrict - parameters.newOrdersPerDistrict) < o_id)
            o_tuples.append(generator.generateOrder(w_id, d_id, o_id, cIdPermutation[o_id - 1], o_ol_cnt, newOrder))

            ## Generate each OrderLine for the order
            for ol_number in range(0, o_ol_cnt):
                ol_tuples.append(generator.generateOrderLine(w_id, d_id, o_id, ol_number, parameters.items, newOrder))
            ## FOR

            ## This is a new order: make one for it
            if newOrder: no_tuples.append([o_id, d_id, w_id])
        ## FOR
        
        handle.loadTuples(TABLENAME_CUSTOMER, c_tuples)
        handle.loadTuples(TABLENAME_ORDERS, o_tuples)
        handle.loadTuples(TABLENAME_ORDER_LINE, ol_tuples)
        handle.loadTuples(TABLENAME_NEW_ORDER, no_tuples)
    ## FOR
    handle.loadTuples(TABLENAME_DISTRICT, d_tuples)
    
    ## Select 10% of the stock to be marked "original"
    s_tuples = [ ]
    selectedRows = rand.selectUniqueIds(parameters.items / 10, 1, parameters.items)
    for i_id in range(1, parameters.items):
        original = (i_id in selectedRows)
        s_tuples.append(generator.generateStock(w_id, i_id, original))
    ## FOR
    handle.loadTuples(TABLENAME_STOCK, s_tuples)
    
## DEF


## ==============================================
## executeDriver
## ==============================================
def executeDriver(handle, duration, warehouses, scalefactor):
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
                val = (options[key][0].lower == "true")
            else: 
                val = orig_type(options[key][0])
            globals()[varname] = val
            logging.debug("%s = %s" % (varname, str(globals()[varname])))
    ## FOR
    if OPT_DEBUG: logging.getLogger().setLevel(logging.DEBUG)
    
    ## Create a handle to the client driver
    system_target = args[0]
    assert system_target, "Missing target system name"
    
    system_config = { "directory": "/tmp" }
    #config_target = args[1]
    #assert config_target, "Missing target system configuration path"
    #with open(config_target, "r") as f:
        #json_config = json.load(f)
    
    ## Create ScaleParameters
    parameters = scaleparameters.makeWithScaleFactor(OPT_WAREHOUSES, OPT_SCALEFACTOR)
    nurand = rand.setNURand(nurand.makeForLoad())
    
    handle = create_handle(system_target)
    assert handle != None, "Failed to create '%s' handle" % system_target
    print handle
    handle.loadConfig(system_config)
    
    ## DATA LOADER!!!
    executeLoader(handle, parameters)
    sys.exit(1)
    
    ## WORKLOAD DRIVER!!!
    results = executeDriver(handle, results, OPT_WAREHOUSES, OPT_SCALEFACTOR)
    
    print results
    
## MAIN