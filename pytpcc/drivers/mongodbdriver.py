# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
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

from __future__ import with_statement

import os
import logging
import pymongo
from pprint import pprint,pformat

import constants
from abstractdriver import *

TABLE_COLUMNS = {
    constants.TABLENAME_ITEM: [
        "I_ID", # INTEGER
        "I_IM_ID", # INTEGER
        "I_NAME", # VARCHAR
        "I_PRICE", # FLOAT
        "I_DATA", # VARCHAR
    ],
    constants.TABLENAME_WAREHOUSE: [
        "W_ID", # SMALLINT
        "W_NAME", # VARCHAR
        "W_STREET_1", # VARCHAR
        "W_STREET_2", # VARCHAR
        "W_CITY", # VARCHAR
        "W_STATE", # VARCHAR
        "W_ZIP", # VARCHAR
        "W_TAX", # FLOAT
        "W_YTD", # FLOAT
    ],    
    constants.TABLENAME_DISTRICT: [
        "D_ID", # TINYINT
        "D_W_ID", # SMALLINT
        "D_NAME", # VARCHAR
        "D_STREET_1", # VARCHAR
        "D_STREET_2", # VARCHAR
        "D_CITY", # VARCHAR
        "D_STATE", # VARCHAR
        "D_ZIP", # VARCHAR
        "D_TAX", # FLOAT
        "D_YTD", # FLOAT
        "D_NEXT_O_ID", # INT
    ],
    constants.TABLENAME_CUSTOMER:   [
        "C_ID", # INTEGER
        "C_D_ID", # TINYINT
        "C_W_ID", # SMALLINT
        "C_FIRST", # VARCHAR
        "C_MIDDLE", # VARCHAR
        "C_LAST", # VARCHAR
        "C_STREET_1", # VARCHAR
        "C_STREET_2", # VARCHAR
        "C_CITY", # VARCHAR
        "C_STATE", # VARCHAR
        "C_ZIP", # VARCHAR
        "C_PHONE", # VARCHAR
        "C_SINCE", # TIMESTAMP
        "C_CREDIT", # VARCHAR
        "C_CREDIT_LIM", # FLOAT
        "C_DISCOUNT", # FLOAT
        "C_BALANCE", # FLOAT
        "C_YTD_PAYMENT", # FLOAT
        "C_PAYMENT_CNT", # INTEGER
        "C_DELIVERY_CNT", # INTEGER
        "C_DATA", # VARCHAR
    ],
    constants.TABLENAME_STOCK:      [
        "S_I_ID", # INTEGER
        "S_W_ID", # SMALLINT
        "S_QUANTITY", # INTEGER
        "S_DIST_01", # VARCHAR
        "S_DIST_02", # VARCHAR
        "S_DIST_03", # VARCHAR
        "S_DIST_04", # VARCHAR
        "S_DIST_05", # VARCHAR
        "S_DIST_06", # VARCHAR
        "S_DIST_07", # VARCHAR
        "S_DIST_08", # VARCHAR
        "S_DIST_09", # VARCHAR
        "S_DIST_10", # VARCHAR
        "S_YTD", # INTEGER
        "S_ORDER_CNT", # INTEGER
        "S_REMOTE_CNT", # INTEGER
        "S_DATA", # VARCHAR
    ],
    constants.TABLENAME_ORDERS:     [
        "O_ID", # INTEGER
        "O_D_ID", # TINYINT
        "O_W_ID", # SMALLINT
        "O_C_ID", # INTEGER
        "O_ENTRY_D", # TIMESTAMP
        "O_CARRIER_ID", # INTEGER
        "O_OL_CNT", # INTEGER
        "O_ALL_LOCAL", # INTEGER
    ],
    constants.TABLENAME_NEW_ORDER:  [
        "NO_O_ID", # INTEGER
        "NO_D_ID", # TINYINT
        "NO_W_ID", # SMALLINT
    ],
    constants.TABLENAME_ORDER_LINE: [
        "OL_O_ID", # INTEGER
        "OL_D_ID", # TINYINT
        "OL_W_ID", # SMALLINT
        "OL_NUMBER", # INTEGER
        "OL_I_ID", # INTEGER
        "OL_SUPPLY_W_ID", # SMALLINT
        "OL_DELIVERY_D", # TIMESTAMP
        "OL_QUANTITY", # INTEGER
        "OL_AMOUNT", # FLOAT
        "OL_DIST_INFO", # VARCHAR
    ],
    constants.TABLENAME_HISTORY:    [
        "H_C_ID", # INTEGER
        "H_C_D_ID", # TINYINT
        "H_C_W_ID", # SMALLINT
        "H_D_ID", # TINYINT
        "H_W_ID", # SMALLINT
        "H_DATE", # TIMESTAMP
        "H_AMOUNT", # FLOAT
        "H_DATA", # VARCHAR
    ],
}
TABLE_INDEXES = {
    constants.TABLENAME_ITEM: [
        "I_ID",
    ],
    constants.TABLENAME_WAREHOUSE: [
        "W_ID",
    ],    
    constants.TABLENAME_DISTRICT: [
        "D_ID",
        "D_W_ID",
    ],
    constants.TABLENAME_CUSTOMER:   [
        "C_ID",
        "C_D_ID",
        "C_W_ID",
    ],
    constants.TABLENAME_STOCK:      [
        "S_I_ID",
        "S_W_ID",
    ],
    constants.TABLENAME_ORDERS:     [
        "O_ID",
        "O_D_ID",
        "O_W_ID",
        "O_C_ID",
    ],
    constants.TABLENAME_NEW_ORDER:  [
        "NO_O_ID",
        "NO_D_ID",
        "NO_W_ID",
    ],
    constants.TABLENAME_ORDER_LINE: [
        "OL_O_ID",
        "OL_D_ID",
        "OL_W_ID",
    ],
}

## ==============================================
## MongodbDriver
## ==============================================
class MongodbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "host": ("The hostname to mongod", "localhost" ),
        "port": ("The port number to mongod", 27017 ),
        "name": ("Collection name", "tpcc"),
    }
    
    def __init__(self, ddl):
        super(MongodbDriver, self).__init__("mongodb", ddl)
        self.database = None
        self.conn = None
        self.cursor = None
        
        ## Create member mapping to collections
        for name in constants.ALL_TABLES:
            self.__dict__[name.lower()] = None
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return MongodbDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in MongodbDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.conn = pymongo.Connection(config['host'], config['port'])
        self.database = self.conn[str(config['name'])]
        
        if config["reset"]:
            logging.info("Deleting database '%s'" % self.database.name)
            for name in constants.ALL_TABLES:
                if name in self.database.collection_names():
                    self.database.drop_collection(name)
                    logging.debug("Dropped collection %s" % name)
        ## IF
        
        ## Setup!
        for name in constants.ALL_TABLES:
            ## Create member mapping to collections
            self.__dict__[name.lower()] = self.database[name]
            
            ## Create Indexes
            if name in TABLE_INDEXES:
                self.database[name].ensure_index(map(lambda x: (x, pymongo.ASCENDING), TABLE_INDEXES[name]))
        ## FOR
    
    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        
        assert tableName in TABLE_COLUMNS, "Unexpected table %s" % tableName
        columns = TABLE_COLUMNS[tableName]
        num_columns = len(columns)
        
        tuple_dicts = [ ]
        for t in tuples:
            tuple_dicts.append(dict(map(lambda i: (columns[i], t[i]), range(num_columns))))
        ## FOR
        
        table = self.database[tableName]
        table.insert(tuple_dicts)
        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        
        return

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Finished loading tables")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            for name in constants.ALL_TABLES:
                logging.debug("%-12s%d records" % (name+":", self.database[name].count()))
        ## IF

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            ## getNewOrder
            newOrder = self.new_order.find_one({"D_ID": d_id, "D_W_ID": w_id}, {"no_o_id": 1})
            if newOrder == None:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            assert len(newOrder) > 0
            no_o_id = neworder["no_o_id"]
            
            ## getCId
            customer = self.orders.find_one({"O_ID": no_o_id, "O_D_ID": d_id, "O_W_ID": w_id}, {"C_ID": 1})
            assert customer != None
            c_id = customer["C_ID"]

            ## sumOLAmount
            orderLines = self.order_line.find({"OL_O_ID": no_o_id, "OL_D_ID": d_id, "OL_W_ID": w_id},
                                              {"OL_AMOUNT": 1})
            ol_total = 0.0
            for ol in orderLines:
                ol_total += ol["OL_AMOUNT"]
            ## FOR

            ## deleteNewOrder
            self.new_order.remove({"NO_D_ID": d_id, "NO_W_ID": w_id, "NO_O_ID": no_o_id})
            
            ## updateOrders
            self.orders.update({"O_ID": no_o_id, "O_D_ID": d_id, "O_W_ID": w_id}, {"$set": {"O_CARRIER_ID": o_carrier_id}}, multi=False)
            
            ## updateOrderLine
            self.order_line.update({"OL_O_ID": no_o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}, {"$set": {"OL_DELIVERY_D": ol_delivery_d}}, multi=False)
            
            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            ## updateCustomer
            self.customer.update({"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"$inc": {"C_BALANCE": ol_total}})

            result.append((d_id, no_o_id))
        ## FOR
        return result

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
        s_dist_col = "S_DIST_%02d" % d_id
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        ## http://stackoverflow.com/q/3844931/
        all_local = (not i_w_ids or [w_id] * len(i_w_ids) == i_w_ids)
        
        items = self.item.find({"I_ID": {"$in": i_ids}}, {"I_ID": 1, "I_PRICE": 1, "I_NAME": 1, "I_DATA": 1})
        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        if items.count() != len(i_ids):
            ## TODO Abort here!
            return
        ## IF
        
        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        
        # getWarehouseTaxRate
        w = self.warehouse.find_one({"W_ID": w_id}, {"W_TAX": 1})
        assert w
        w_tax = w["W_TAX"]
        
        # getDistrict
        d = self.district.find_one({"D_ID": d_id, "D_W_ID": w_id}, {"D_TAX": 1, "D_NEXT_O_ID": 1})
        assert d
        d_tax = d["D_TAX"]
        d_next_o_id = d["D_NEXT_O_ID"]
        
        # incrementNextOrderId
        # HACK: This is not transactionally safe!
        self.district.update(d, {"$inc": {"D_NEXT_O_ID": 1}}, multi=False)
        
        # getCustomer
        c = self.customer.find_one({"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"C_DISCOUNT": 1, "C_LAST": 1, "C_CREDIT": 1})
        assert c
        c_discount = c["C_DISCOUNT"]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        
        # createOrder
        self.orders.insert({"O_ID": d_next_o_id, "O_D_ID": d_id, "O_W_ID": w_id, "O_C_ID": c_id, "O_ENTRY_D": o_entry_d, "O_CARRIER_ID": o_carrier_id, "O_OL_CNT": ol_cnt, "O_ALL_LOCAL": all_local})  
        
        # createNewOrder
        self.new_order.insert({"NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id})

        ## ----------------
        ## OPTIMIZATION:
        ## If all of the items are at the same warehouse, then we'll issue a single
        ## request to get their information
        ## ----------------
        stockInfos = None
        if all_local and False:
            # getStockInfo
            allStocks = self.stock.find({"S_I_ID": {"$in": i_ids}, "S_W_ID": w_id}, {"S_I_ID": 1, "S_QUANTITY": 1, "S_DATA": 1, "S_YTD": 1, "S_ORDER_CNT": 1, "S_REMOTE_CNT": 1, s_dist_col: 1})
            assert allStocks.count() == ol_cnt
            stockInfos = { }
            for si in allStocks:
                stockInfos["S_I_ID"] = si # HACK
        ## IF

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        item_data = [ ]
        total = 0
        for i in range(ol_cnt):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            itemInfo = items[i]
            i_name = itemInfo["I_NAME"]
            i_data = itemInfo["I_DATA"]
            i_price = itemInfo["I_PRICE"]

            # getStockInfo
            if all_local and stockInfos != None:
                si = stockInfos[ol_i_id]
                assert si["S_I_ID"] == ol_i_id, "S_I_ID should be %d\n%s" % (ol_i_id, pformat(si))
            else:
                si = self.stock.find_one({"S_I_ID": ol_i_id, "S_W_ID": w_id}, {"S_I_ID": 1, "S_QUANTITY": 1, "S_DATA": 1, "S_YTD": 1, "S_ORDER_CNT": 1, "S_REMOTE_CNT": 1, s_dist_col: 1})
            assert si, "Failed to find S_I_ID: %d\n%s" % (ol_i_id, pformat(itemInfo))

            s_quantity = si["S_QUANTITY"]
            s_ytd = si["S_YTD"]
            s_order_cnt = si["S_ORDER_CNT"]
            s_remote_cnt = si["S_REMOTE_CNT"]
            s_data = si["S_DATA"]
            s_dist_xx = si[s_dist_col] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            
            if ol_supply_w_id != w_id: s_remote_cnt += 1

            # updateStock
            self.stock.update(si, {"$set": {"S_QUANTITY": s_quantity, "S_YTD": s_ytd, "S_ORDER_CNT": s_order_cnt, "S_REMOTE_CNT": s_remote_cnt}})

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            # createOrderLine
            self.order_line.insert({"OL_O_ID": d_next_o_id, "OL_D_ID": d_id, "OL_W_ID": w_id, "OL_NUMBER": ol_number, "OL_I_ID": ol_i_id, "OL_SUPPLY_W_ID": ol_supply_w_id, "OL_DELIVERY_D": o_entry_d, "OL_QUANTITY": ol_quantity, "OL_AMOUNT": ol_amount, "OL_DIST_INFO": s_dist_xx})

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR
        
        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        
        return [ c, misc, item_data ]

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        if c_id != None:
            # getCustomerByCustomerId
            c = self.customer.find_one({"C_W_ID": w_id, "C_D_ID": d_id, "C_ID": c_id}, {"C_ID": 1, "C_FIRST": 1, "C_MIDDLE": 1, "C_LAST": 1, "C_BALANCE": 1})
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            all_customers = self.customer.find({"C_W_ID": w_id, "C_D_ID": d_id, "C_LAST": c_last}, {"C_ID": 1, "C_FIRST": 1, "C_MIDDLE": 1, "C_LAST": 1, "C_BALANCE": 1})
            namecnt = all_customers.count()
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        assert len(c) > 0
        assert c_id != None

        # getLastOrder
        order = self.orders.find({"O_W_ID": w_id, "O_D_ID": d_id, "O_C_ID": c_id}, {"O_ID": 1, "O_CARRIER_ID": 1, "O_ENTRY_D": 1}).sort("O_ID", direction=pymongo.DESCENDING).limit(1)[0]
        o_id = order["O_ID"]

        if order:
            # getOrderLines
            orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": o_id}, {"OL_SUPPLY_W_ID": 1, "OL_I_ID": 1, "OL_QUANTITY": 1, "OL_AMOUNT": 1, "OL_DELIVERY_D": 1})
        else:
            orderLines = [ ]

        return [ c, order, orderLines ]

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        if c_id != None:
            # getCustomerByCustomerId
            c = self.customer.find_one({"C_W_ID": w_id, "C_D_ID": d_id, "C_ID": c_id})
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            all_customers = self.customer.find({"C_W_ID": w_id, "C_D_ID": d_id, "C_LAST": c_last})
            namecnt = all_customers.count()
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        assert len(c) > 0
        assert c_id != None
        c_balance = c["C_BALANCE"] - h_amount
        c_ytd_payment = c["C_YTD_PAYMENT"] + h_amount
        c_payment_cnt = c["C_PAYMENT_CNT"] + 1
        c_data = c["C_DATA"]

        # getWarehouse
        w = self.warehouse.find_one({"W_ID": w_id}, {"W_NAME": 1, "W_STREET_1": 1, "W_STREET_2": 1, "W_CITY": 1, "W_STATE": 1, "W_ZIP": 1})
        assert w
        
        # getDistrict
        d = self.district.find_one({"D_W_ID": w_id, "D_ID": d_id}, {"D_NAME": 1, "D_STREET_1": 1, "D_STREET_2": 1, "D_CITY": 1, "D_STATE": 1, "D_ZIP": 1})
        assert d
        
        # updateWarehouseBalance
        self.warehouse.update(w, {"$inc": {"H_AMOUNT": h_amount}})
        
        # updateDistrictBalance
        self.district.update(d, {"$inc": {"D_YTD": h_amount}})
        
        # Customer Credit Information
        if c["C_CREDIT"] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            
            # updateBCCustomer
            self.customer.update(c, {"$set": {"C_BALANCE": c_balance, "C_YTD_PAYMENT": c_ytd_payment, "C_PAYMENT_CNT": c_payment_cnt, "C_DATA": c_data}})
        else:
            c_data = ""
            # updateGCCustomer
            self.customer.update(c, {"$set": {"C_BALANCE": c_balance, "C_YTD_PAYMENT": c_ytd_payment, "C_PAYMENT_CNT": c_payment_cnt}})

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (w["W_NAME"], d["D_NAME"])
        
        # insertHistory
        self.history.insert({"H_C_ID": c_id, "H_C_D_ID": c_d_id, "H_C_W_ID": c_w_id, "H_D_ID": d_id, "H_W_ID": w_id, "H_DATE": h_date, "H_AMOUNT": h_amount, "H_DATA": h_data})

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return [ w, d, c ]
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        # getOId
        d = self.district.find_one({"D_W_ID": w_id, "D_ID": d_id}, {"D_NEXT_O_ID": 1})
        assert d
        o_id = d["D_NEXT_O_ID"]
        
        # getStockCount
        # Outer Table: ORDER_LINE
        # Inner Table: STOCK
        orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": {"$lt": o_id, "$gte": o_id-20}}, {"OL_I_ID": 1})
        assert orderLines
        ol_ids = set()
        for ol in orderLines:
            ol_ids.add(ol["OL_I_ID"])
        ## FOR
        result = self.stock.find({"S_W_ID": w_id, "S_I_ID": {"$in": list(ol_ids)}, "S_QUANTITY": {"$lt": threshold}}).count()
        
        return int(result)
        
## CLASS