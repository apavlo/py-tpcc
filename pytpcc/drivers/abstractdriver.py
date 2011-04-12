#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
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

## ==============================================
## AbstractDriver
## ==============================================
class AbstractDriver(object):
    def __init__(self, name):
        self.name = name
        self.config = None
    
    def loadConfig(self, config):
        self.config = config
        
    def loadTuples(self, table, tuples):
        """Load a list of tuples into the target table"""
        raise NotImplementedError("%s does not loadTuples" % (self.name))
        
    def executeTransaction(self, txn, params):
        """Execute a transaction based on the given name"""
        
        if constants.TransactionTypes.DELIVERY == txn:
            result = doDelivery(self, params)
        elif constants.TransactionTypes.NEW_ORDER == txn:
            result = doNewOrder(self, params)
        elif constants.TransactionTypes.ORDER_STATUS == txn:
            result = doOrderStatus(self, params)
        elif constants.TransactionTypes.PAYMENT == txn:
            result = doPayment(self, params)
        elif constants.TransactionTypes.STOCK_LEVEL == txn:
            result = doStockLevel(self, params)
        else:
            assert False, "Unexpected TransactionType: " + txn
        return
        
    def doDelivery(self, params):
        """Execute DELIVERY Transaction
        Parameters Dict:
            w_id
            o_carrier_id
            ol_delivery_d
        """
        raise NotImplementedError("%s does not implement DELIVERY" % (self.name))
    
    def doNewOrder(self, params):
        """Execute NEW_ORDER Transaction
        Parameters Dict:
            w_id
            d_id
            c_id
            o_entry_d
            i_ids
            i_w_ids
            i_qtys
        """
        raise NotImplementedError("%s does not implement NEW_ORDER" % (self.name))

    def doOrderStatus(self, params):
        """Execute ORDER_STATUS Transaction
        Parameters Dict:
            w_id
            d_id
            c_id
            c_last
        """
        raise NotImplementedError("%s does not implement ORDER_STATUS" % (self.name))


    def doPayment(self, params):
        """Execute PAYMENT Transaction
        Parameters Dict:
            w_id
            d_id
            h_amount
            c_w_id
            c_d_id
            c_id
            c_last
            h_date
        """
        raise NotImplementedError("%s does not implement PAYMENT" % (self.name))


    def doStockLevel(self, params):
        """Execute STOCK_LEVEL Transaction
        Parameters Dict:
            w_id
            d_id
            threshold
        """
        raise NotImplementedError("%s does not implement STOCK_LEVEL" % (self.name))
## CLASS