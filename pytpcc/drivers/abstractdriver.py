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

from datetime import datetime

import constants

## ==============================================
## AbstractDriver
## ==============================================
class AbstractDriver(object):
    def __init__(self, name, ddl):
        self.name = name
        self.driver_name = "%sDriver" % self.name.title()
        self.ddl = ddl
        
    def __str__(self):
        return self.driver_name
    
    def makeDefaultConfig(self):
        """This function needs to be implemented by all sub-classes.
        It should return the items that need to be in your implementation's configuration file.
        Each item in the list is a triplet containing: ( <PARAMETER NAME>, <DESCRIPTION>, <DEFAULT VALUE> )
        """
        raise NotImplementedError("%s does not implement makeDefaultConfig" % (self.driver_name))
    
    def loadConfig(self, config):
        """Initialize the driver using the given configuration dict"""
        raise NotImplementedError("%s does not implement loadConfig" % (self.driver_name))
        
    def formatConfig(self, config):
        """Return a formatted version of the config dict that can be used with the --config command line argument"""
        ret =  "# %s Configuration File\n" % (self.driver_name)
        ret += "# Created %s\n" % (datetime.now())
        ret += "[%s]" % self.name
        
        for name in config.keys():
            desc, default = config[name]
            if default == None: default = ""
            ret += "\n\n# %s\n%-20s = %s" % (desc, name, default) 
        return (ret)
        
    def loadStart(self):
        """Optional callback to indicate to the driver that the data loading phase is about to begin."""
        return None
        
    def loadFinish(self):
        """Optional callback to indicate to the driver that the data loading phase is finished."""
        return None

    def loadFinishItem(self):
        """Optional callback to indicate to the driver that the ITEM data has been passed to the driver."""
        return None

    def loadFinishWarehouse(self, w_id):
        """Optional callback to indicate to the driver that the data for the given warehouse is finished."""
        return None
        
    def loadFinishDistrict(self, w_id, d_id):
        """Optional callback to indicate to the driver that the data for the given district is finished."""
        return None
        
    def loadTuples(self, tableName, tuples):
        """Load a list of tuples into the target table"""
        raise NotImplementedError("%s does not implement loadTuples" % (self.driver_name))
        
    def executeStart(self):
        """Optional callback before the execution phase starts"""
        return None
        
    def executeFinish(self):
        """Callback after the execution phase finishes"""
        return None
        
    def executeTransaction(self, txn, params):
        """Execute a transaction based on the given name"""
        
        if constants.TransactionTypes.DELIVERY == txn:
            result = self.doDelivery(params)
        elif constants.TransactionTypes.NEW_ORDER == txn:
            result = self.doNewOrder(params)
        elif constants.TransactionTypes.ORDER_STATUS == txn:
            result = self.doOrderStatus(params)
        elif constants.TransactionTypes.PAYMENT == txn:
            result = self.doPayment(params)
        elif constants.TransactionTypes.STOCK_LEVEL == txn:
            result = self.doStockLevel(params)
        else:
            assert False, "Unexpected TransactionType: " + txn
        return result
        
    def doDelivery(self, params):
        """Execute DELIVERY Transaction
        Parameters Dict:
            w_id
            o_carrier_id
            ol_delivery_d
        """
        raise NotImplementedError("%s does not implement doDelivery" % (self.driver_name))
    
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
        raise NotImplementedError("%s does not implement doNewOrder" % (self.driver_name))

    def doOrderStatus(self, params):
        """Execute ORDER_STATUS Transaction
        Parameters Dict:
            w_id
            d_id
            c_id
            c_last
        """
        raise NotImplementedError("%s does not implement doOrderStatus" % (self.driver_name))

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
        raise NotImplementedError("%s does not implement doPayment" % (self.driver_name))

    def doStockLevel(self, params):
        """Execute STOCK_LEVEL Transaction
        Parameters Dict:
            w_id
            d_id
            threshold
        """
        raise NotImplementedError("%s does not implement doStockLevel" % (self.driver_name))
## CLASS