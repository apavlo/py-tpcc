# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http:##www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
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

import os
import sys

import logging
from datetime import datetime
from random import shuffle
from pprint import pprint,pformat

import constants
from util import *

class Loader:
    
    def __init__(self, handle, scaleParameters, w_ids, needLoadItems):
        self.handle = handle
        self.scaleParameters = scaleParameters
        self.w_ids = w_ids
        self.needLoadItems = needLoadItems
        self.batch_size = 2500
        
    ## ==============================================
    ## execute
    ## ==============================================
    def execute(self):
        
        ## Item Table
        if self.needLoadItems:
            logging.debug("Loading ITEM table")
            self.loadItems()
            self.handle.loadFinishItem()
            
        ## Then create the warehouse-specific tuples
        for w_id in self.w_ids:
            self.loadWarehouse(w_id)
            self.handle.loadFinishWarehouse(w_id)
        ## FOR
        
        return (None)

    ## ==============================================
    ## loadItems
    ## ==============================================
    def loadItems(self):
        ## Select 10% of the rows to be marked "original"
        originalRows = rand.selectUniqueIds(self.scaleParameters.items / 10, 1, self.scaleParameters.items)
        
        ## Load all of the items
        tuples = [ ]
        total_tuples = 0
        for i in range(1, self.scaleParameters.items+1):
            original = (i in originalRows)
            tuples.append(self.generateItem(i, original))
            total_tuples += 1
            if len(tuples) == self.batch_size:
                logging.debug("LOAD - %s: %5d / %d" % (constants.TABLENAME_ITEM, total_tuples, self.scaleParameters.items))
                self.handle.loadTuples(constants.TABLENAME_ITEM, tuples)
                tuples = [ ]
        ## FOR
        if len(tuples) > 0:
            logging.debug("LOAD - %s: %5d / %d" % (constants.TABLENAME_ITEM, total_tuples, self.scaleParameters.items))
            self.handle.loadTuples(constants.TABLENAME_ITEM, tuples)
    ## DEF

    ## ==============================================
    ## loadWarehouse
    ## ==============================================
    def loadWarehouse(self, w_id):
        logging.debug("LOAD - %s: %d / %d" % (constants.TABLENAME_WAREHOUSE, w_id, len(self.w_ids)))
        
        ## WAREHOUSE
        w_tuples = [ self.generateWarehouse(w_id) ]
        self.handle.loadTuples(constants.TABLENAME_WAREHOUSE, w_tuples)

        ## DISTRICT
        d_tuples = [ ]
        for d_id in range(1, self.scaleParameters.districtsPerWarehouse+1):
            d_next_o_id = self.scaleParameters.customersPerDistrict + 1
            d_tuples = [ self.generateDistrict(w_id, d_id, d_next_o_id) ]
            
            c_tuples = [ ]
            h_tuples = [ ]
            
            ## Select 10% of the customers to have bad credit
            selectedRows = rand.selectUniqueIds(self.scaleParameters.customersPerDistrict / 10, 1, self.scaleParameters.customersPerDistrict)
            
            ## TPC-C 4.3.3.1. says that o_c_id should be a permutation of [1, 3000]. But since it
            ## is a c_id field, it seems to make sense to have it be a permutation of the
            ## customers. For the "real" thing this will be equivalent
            cIdPermutation = [ ]

            for c_id in range(1, self.scaleParameters.customersPerDistrict+1):
                badCredit = (c_id in selectedRows)
                c_tuples.append(self.generateCustomer(w_id, d_id, c_id, badCredit, True))
                h_tuples.append(self.generateHistory(w_id, d_id, c_id))
                cIdPermutation.append(c_id)
            ## FOR
            assert cIdPermutation[0] == 1
            assert cIdPermutation[self.scaleParameters.customersPerDistrict - 1] == self.scaleParameters.customersPerDistrict
            shuffle(cIdPermutation)
            
            o_tuples = [ ]
            ol_tuples = [ ]
            no_tuples = [ ]
            
            for o_id in range(1, self.scaleParameters.customersPerDistrict+1):
                o_ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)
                
                ## The last newOrdersPerDistrict are new orders
                newOrder = ((self.scaleParameters.customersPerDistrict - self.scaleParameters.newOrdersPerDistrict) < o_id)
                o_tuples.append(self.generateOrder(w_id, d_id, o_id, cIdPermutation[o_id - 1], o_ol_cnt, newOrder))

                ## Generate each OrderLine for the order
                for ol_number in range(0, o_ol_cnt):
                    ol_tuples.append(self.generateOrderLine(w_id, d_id, o_id, ol_number, self.scaleParameters.items, newOrder))
                ## FOR

                ## This is a new order: make one for it
                if newOrder: no_tuples.append([o_id, d_id, w_id])
            ## FOR
            
            self.handle.loadTuples(constants.TABLENAME_DISTRICT, d_tuples)
            self.handle.loadTuples(constants.TABLENAME_CUSTOMER, c_tuples)
            self.handle.loadTuples(constants.TABLENAME_ORDERS, o_tuples)
            self.handle.loadTuples(constants.TABLENAME_ORDER_LINE, ol_tuples)
            self.handle.loadTuples(constants.TABLENAME_NEW_ORDER, no_tuples)
            self.handle.loadTuples(constants.TABLENAME_HISTORY, h_tuples)
            self.handle.loadFinishDistrict(w_id, d_id)
        ## FOR
        
        ## Select 10% of the stock to be marked "original"
        s_tuples = [ ]
        selectedRows = rand.selectUniqueIds(self.scaleParameters.items / 10, 1, self.scaleParameters.items)
        total_tuples = 0
        for i_id in range(1, self.scaleParameters.items+1):
            original = (i_id in selectedRows)
            s_tuples.append(self.generateStock(w_id, i_id, original))
            if len(s_tuples) >= self.batch_size:
                logging.debug("LOAD - %s [W_ID=%d]: %5d / %d" % (constants.TABLENAME_STOCK, w_id, total_tuples, self.scaleParameters.items))
                self.handle.loadTuples(constants.TABLENAME_STOCK, s_tuples)
                s_tuples = [ ]
            total_tuples += 1
        ## FOR
        if len(s_tuples) > 0:
            logging.debug("LOAD - %s [W_ID=%d]: %5d / %d" % (constants.TABLENAME_STOCK, w_id, total_tuples, self.scaleParameters.items))
            self.handle.loadTuples(constants.TABLENAME_STOCK, s_tuples)
    ## DEF

    ## ==============================================
    ## generateItem
    ## ==============================================
    def generateItem(self, id, original):
        i_id = id
        i_im_id = rand.number(constants.MIN_IM, constants.MAX_IM)
        i_name = rand.astring(constants.MIN_I_NAME, constants.MAX_I_NAME)
        i_price = rand.fixedPoint(constants.MONEY_DECIMALS, constants.MIN_PRICE, constants.MAX_PRICE)
        i_data = rand.astring(constants.MIN_I_DATA, constants.MAX_I_DATA)
        if original: i_data = self.fillOriginal(i_data)

        return [i_id, i_im_id, i_name, i_price, i_data]
    ## DEF

    ## ==============================================
    ## generateWarehouse
    ## ==============================================
    def generateWarehouse(self, w_id):
        w_tax = self.generateTax()
        w_ytd = constants.INITIAL_W_YTD
        w_address = self.generateAddress()
        return [w_id] + w_address + [w_tax, w_ytd]
    ## DEF

    ## ==============================================
    ## generateDistrict
    ## ==============================================
    def generateDistrict(self, d_w_id, d_id, d_next_o_id):
        d_tax = self.generateTax()
        d_ytd = constants.INITIAL_D_YTD
        d_address = self.generateAddress()
        return [d_id, d_w_id] + d_address + [d_tax, d_ytd, d_next_o_id]
    ## DEF

    ## ==============================================
    ## generateCustomer
    ## ==============================================
    def generateCustomer(self, c_w_id, c_d_id, c_id, badCredit, doesReplicateName):
        c_first = rand.astring(constants.MIN_FIRST, constants.MAX_FIRST)
        c_middle = constants.MIDDLE

        assert 1 <= c_id and c_id <= constants.CUSTOMERS_PER_DISTRICT
        if c_id <= 1000:
            c_last = rand.makeLastName(c_id - 1)
        else:
            c_last = rand.makeRandomLastName(constants.CUSTOMERS_PER_DISTRICT)

        c_phone = rand.nstring(constants.PHONE, constants.PHONE)
        c_since = datetime.now()
        c_credit = constants.BAD_CREDIT if badCredit else constants.GOOD_CREDIT
        c_credit_lim = constants.INITIAL_CREDIT_LIM
        c_discount = rand.fixedPoint(constants.DISCOUNT_DECIMALS, constants.MIN_DISCOUNT, constants.MAX_DISCOUNT)
        c_balance = constants.INITIAL_BALANCE
        c_ytd_payment = constants.INITIAL_YTD_PAYMENT
        c_payment_cnt = constants.INITIAL_PAYMENT_CNT
        c_delivery_cnt = constants.INITIAL_DELIVERY_CNT
        c_data = rand.astring(constants.MIN_C_DATA, constants.MAX_C_DATA)

        c_street1 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
        c_street2 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
        c_city = rand.astring(constants.MIN_CITY, constants.MAX_CITY)
        c_state = rand.astring(constants.STATE, constants.STATE)
        c_zip = self.generateZip()

        return [ c_id, c_d_id, c_w_id, c_first, c_middle, c_last, \
                c_street1, c_street2, c_city, c_state, c_zip, \
                c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, \
                c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data ]
    ## DEF

    ## ==============================================
    ## generateOrder
    ## ==============================================
    def generateOrder(self, o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt, newOrder):
        """Returns the generated o_ol_cnt value."""
        o_entry_d = datetime.now()
        o_carrier_id = constants.NULL_CARRIER_ID if newOrder else rand.number(constants.MIN_CARRIER_ID, constants.MAX_CARRIER_ID)
        o_all_local = constants.INITIAL_ALL_LOCAL
        return [ o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local ]
    ## DEF

    ## ==============================================
    ## generateOrderLine
    ## ==============================================
    def generateOrderLine(self, ol_w_id, ol_d_id, ol_o_id, ol_number, max_items, newOrder):
        ol_i_id = rand.number(1, max_items)
        ol_supply_w_id = ol_w_id
        ol_delivery_d = datetime.now()
        ol_quantity = constants.INITIAL_QUANTITY

        if newOrder == False:
            ol_amount = 0.00
        else:
            ol_amount = rand.fixedPoint(constants.MONEY_DECIMALS, constants.MIN_AMOUNT, constants.MAX_PRICE * constants.MAX_OL_QUANTITY)
            ol_delivery_d = None
        ol_dist_info = rand.astring(constants.DIST, constants.DIST)

        return [ ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info ]
    ## DEF

    ## ==============================================
    ## generateStock
    ## ==============================================
    def generateStock(self, s_w_id, s_i_id, original):
        s_quantity = rand.number(constants.MIN_QUANTITY, constants.MAX_QUANTITY);
        s_ytd = 0;
        s_order_cnt = 0;
        s_remote_cnt = 0;

        s_data = rand.astring(constants.MIN_I_DATA, constants.MAX_I_DATA);
        if original: self.fillOriginal(s_data)

        s_dists = [ ]
        for i in range(0, constants.DISTRICTS_PER_WAREHOUSE):
            s_dists.append(rand.astring(constants.DIST, constants.DIST))
        
        return [ s_i_id, s_w_id, s_quantity ] + \
               s_dists + \
               [ s_ytd, s_order_cnt, s_remote_cnt, s_data ]
    ## DEF

    ## ==============================================
    ## generateHistory
    ## ==============================================
    def generateHistory(self, h_c_w_id, h_c_d_id, h_c_id):
        h_w_id = h_c_w_id
        h_d_id = h_c_d_id
        h_date = datetime.now()
        h_amount = constants.INITIAL_AMOUNT
        h_data = rand.astring(constants.MIN_DATA, constants.MAX_DATA)
        return [ h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data ]
    ## DEF

    ## ==============================================
    ## generateAddress
    ## ==============================================
    def generateAddress(self):
        """
            Returns a name and a street address 
            Used by both generateWarehouse and generateDistrict.
        """
        name = rand.astring(constants.MIN_NAME, constants.MAX_NAME)
        return [ name ] + self.generateStreetAddress()
    ## DEF

    ## ==============================================
    ## generateStreetAddress
    ## ==============================================
    def generateStreetAddress(self):
        """
            Returns a list for a street address
            Used for warehouses, districts and customers.
        """
        street1 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
        street2 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
        city = rand.astring(constants.MIN_CITY, constants.MAX_CITY)
        state = rand.astring(constants.STATE, constants.STATE)
        zip = self.generateZip()

        return [ street1, street2, city, state, zip ]
    ## DEF

    ## ==============================================
    ## generateTax
    ## ==============================================
    def generateTax(self):
        return rand.fixedPoint(constants.TAX_DECIMALS, constants.MIN_TAX, constants.MAX_TAX)
    ## DEF

    ## ==============================================
    ## generateZip
    ## ==============================================
    def generateZip(self):
        length = constants.ZIP_LENGTH - len(constants.ZIP_SUFFIX)
        return rand.nstring(length, length) + constants.ZIP_SUFFIX
    ## DEF

    ## ==============================================
    ## fillOriginal
    ## ==============================================
    def fillOriginal(self, data):
        """
            a string with ORIGINAL_STRING at a random position
        """
        originalLength = len(constants.ORIGINAL_STRING)
        position = rand.number(0, len(data) - originalLength)
        out = data[:position] + constants.ORIGINAL_STRING + data[position + originalLength:]
        assert len(out) == len(data)
        return out
    ## DEF
## CLASS