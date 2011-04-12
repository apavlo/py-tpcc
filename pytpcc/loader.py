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

import logging
from random import shuffle

import generator
import rand
from constants import *

class Loader:
    
    def __init__(self, handle, params):
        self.handle = handle
        self.params = params
        
    ## ==============================================
    ## executeLoader
    ## ==============================================
    def executeLoader(self):
        
        ## Item Table
        self.loadItems()
            
        ## Then create the warehouse-specific tuples
        for w_id in range(self.params.starting_warehouse, self.params.max_w_id+1):
            self.loadWarehouse(w_id)
        ## FOR
        
        return (None)

    ## ==============================================
    ## loadItems
    ## ==============================================
    def loadItems(self):
        batch_size = 1000
        
        ## Select 10% of the rows to be marked "original"
        originalRows = rand.selectUniqueIds(self.params.items / 10, 1, self.params.items)

        
        ## Load all of the items
        tuples = [ ]
        total_tuples = 0
        for i in range(1, self.params.items+1):
            original = (i in originalRows)
            tuples.append(generator.generateItem(i, original))
            total_tuples += 1
            if len(tuples) == batch_size:
                logging.debug("LOAD - %s: %5d / %d" % (TABLENAME_ITEM, total_tuples, self.params.items))
                self.handle.loadTuples(TABLENAME_ITEM, tuples)
                tuples = [ ]
        ## FOR
        if len(tuples) > 0:
            logging.debug("LOAD - %s: %5d / %d" % (TABLENAME_ITEM, total_tuples, self.params.items))
            self.handle.loadTuples(TABLENAME_ITEM, tuples)
    ## DEF

    ## ==============================================
    ## loadWarehouse
    ## ==============================================
    def loadWarehouse(self, w_id):
        logging.debug("LOAD - %s: %d / %d" % (TABLENAME_WAREHOUSE, w_id, self.params.warehouses))
        
        ## WAREHOUSE
        w_tuples = [ generator.generateWarehouse(w_id) ]
        self.handle.loadTuples(TABLENAME_WAREHOUSE, w_tuples)

        ## DISTRICT
        d_tuples = [ ]
        for d_id in range(1, self.params.districtsPerWarehouse+1):
            d_next_o_id = self.params.customersPerDistrict + 1
            d_tuples.append(generator.generateDistrict(w_id, d_id, d_next_o_id))
            
            c_tuples = [ ]
            h_tuples = [ ]
            
            ## Select 10% of the customers to have bad credit
            selectedRows = rand.selectUniqueIds(self.params.customersPerDistrict / 10, 1, self.params.customersPerDistrict)
            
            ## TPC-C 4.3.3.1. says that o_c_id should be a permutation of [1, 3000]. But since it
            ## is a c_id field, it seems to make sense to have it be a permutation of the
            ## customers. For the "real" thing this will be equivalent
            cIdPermutation = [ ]

            for c_id in range(1, self.params.customersPerDistrict+1):
                badCredit = (c_id in selectedRows)
                c_tuples.append(generator.generateCustomer(w_id, d_id, c_id, badCredit, True))
                d_tuples.append(generator.generateHistory(w_id, d_id, c_id))
                cIdPermutation.append(c_id)
            ## FOR
            assert cIdPermutation[0] == 1
            assert cIdPermutation[self.params.customersPerDistrict - 1] == self.params.customersPerDistrict
            shuffle(cIdPermutation)
            
            o_tuples = [ ]
            ol_tuples = [ ]
            no_tuples = [ ]
            
            for o_id in range(1, self.params.customersPerDistrict):
                o_ol_cnt = rand.number(MIN_OL_CNT, MAX_OL_CNT)
                
                ## The last newOrdersPerDistrict are new orders
                newOrder = ((self.params.customersPerDistrict - self.params.newOrdersPerDistrict) < o_id)
                o_tuples.append(generator.generateOrder(w_id, d_id, o_id, cIdPermutation[o_id - 1], o_ol_cnt, newOrder))

                ## Generate each OrderLine for the order
                for ol_number in range(0, o_ol_cnt):
                    ol_tuples.append(generator.generateOrderLine(w_id, d_id, o_id, ol_number, self.params.items, newOrder))
                ## FOR

                ## This is a new order: make one for it
                if newOrder: no_tuples.append([o_id, d_id, w_id])
            ## FOR
            
            self.handle.loadTuples(TABLENAME_CUSTOMER, c_tuples)
            self.handle.loadTuples(TABLENAME_ORDERS, o_tuples)
            self.handle.loadTuples(TABLENAME_ORDER_LINE, ol_tuples)
            self.handle.loadTuples(TABLENAME_NEW_ORDER, no_tuples)
        ## FOR
        self.handle.loadTuples(TABLENAME_DISTRICT, d_tuples)
        
        ## Select 10% of the stock to be marked "original"
        s_tuples = [ ]
        selectedRows = rand.selectUniqueIds(self.params.items / 10, 1, self.params.items)
        for i_id in range(1, self.params.items):
            original = (i_id in selectedRows)
            s_tuples.append(generator.generateStock(w_id, i_id, original))
        ## FOR
        self.handle.loadTuples(TABLENAME_STOCK, s_tuples)
        
    ## DEF
