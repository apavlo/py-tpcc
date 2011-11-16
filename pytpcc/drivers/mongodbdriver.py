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
import sys
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
        "O_C_ID", # INTEGER
        "O_D_ID", # TINYINT
        "O_W_ID", # SMALLINT
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
        "host":         ("The hostname to mongod", "localhost" ),
        "port":         ("The port number to mongod", 27017 ),
        "name":         ("Collection name", "tpcc"),
        "denormalize":  ("If set to true, then the CUSTOMER data will be denormalized into a single document", True),
    }
    DENORMALIZED_TABLES = [
        constants.TABLENAME_CUSTOMER,
        constants.TABLENAME_ORDERS,
        constants.TABLENAME_ORDER_LINE,
        constants.TABLENAME_HISTORY,
    ]
    
    
    def __init__(self, ddl):
        super(MongodbDriver, self).__init__("mongodb", ddl)
        self.database = None
        self.conn = None
        self.denormalize = False
        self.w_customers = { }
        self.w_orders = { }
        
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
        
        self.conn = pymongo.Connection(config['host'], int(config['port']))
        self.database = self.conn[str(config['name'])]
        self.denormalize = config['denormalize']
        if self.denormalize: logging.debug("Using denormalized data model")
        
        if config["reset"]:
            logging.debug("Deleting database '%s'" % self.database.name)
            for name in constants.ALL_TABLES:
                if name in self.database.collection_names():
                    self.database.drop_collection(name)
                    logging.debug("Dropped collection %s" % name)
        ## IF
        
        ## Setup!
        load_indexes = ('execute' in config and not config['execute']) and \
                       ('load' in config and not config['load'])
        for name in constants.ALL_TABLES:
            if self.denormalize and name in MongodbDriver.DENORMALIZED_TABLES[1:]: continue
            
            ## Create member mapping to collections
            self.__dict__[name.lower()] = self.database[name]
            
            ## Create Indexes
            if load_indexes and name in TABLE_INDEXES and \
            (self.denormalize or (self.denormalize == False and not name in MongodbDriver.DENORMALIZED_TABLES[1:])):
                logging.debug("Creating index for %s" % name)
                for index in TABLE_INDEXES[name]:
                	self.database[name].create_index(index)
        ## FOR
    
    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        logging.debug("Loading %d tuples for tableName %s" % (len(tuples), tableName))
        
        assert tableName in TABLE_COLUMNS, "Unexpected table %s" % tableName
        columns = TABLE_COLUMNS[tableName]
        num_columns = range(len(columns))
        
        tuple_dicts = [ ]
        
        ## We want to combine all of a CUSTOMER's ORDERS, ORDER_LINE, and HISTORY records
        ## into a single document
        if self.denormalize and tableName in MongodbDriver.DENORMALIZED_TABLES:
            ## If this is the CUSTOMER table, then we'll just store the record locally for now
            if tableName == constants.TABLENAME_CUSTOMER:
                for t in tuples:
                    key = tuple(t[:3]) # C_ID, D_ID, W_ID
                    self.w_customers[key] = dict(map(lambda i: (columns[i], t[i]), num_columns))
                ## FOR
                
            ## If this is an ORDER_LINE record, then we need to stick it inside of the 
            ## right ORDERS record
            elif tableName == constants.TABLENAME_ORDER_LINE:
                for t in tuples:
                    o_key = tuple(t[:3]) # O_ID, O_D_ID, O_W_ID
                    (c_key, o_idx) = self.w_orders[o_key]
                    c = self.w_customers[c_key]
                    assert o_idx >= 0
                    assert o_idx < len(c[constants.TABLENAME_ORDERS])
                    o = c[constants.TABLENAME_ORDERS][o_idx]
                    if not tableName in o: o[tableName] = [ ]
                    o[tableName].append(dict(map(lambda i: (columns[i], t[i]), num_columns[4:])))
                ## FOR
                    
            ## Otherwise we have to find the CUSTOMER record for the other tables
            ## and append ourselves to them
            else:
                if tableName == constants.TABLENAME_ORDERS:
                    key_start = 1
                    cols = num_columns[0:1] + num_columns[4:] # Removes O_C_ID, O_D_ID, O_W_ID
                else:
                    key_start = 0
                    cols = num_columns[3:] # Removes H_C_ID, H_C_D_ID, H_C_W_ID
                    
                for t in tuples:
                    c_key = tuple(t[key_start:key_start+3]) # C_ID, D_ID, W_ID
                    assert c_key in self.w_customers, "Customer Key: %s\nAll Keys:\n%s" % (str(c_key), "\n".join(map(str, sorted(self.w_customers.keys()))))
                    c = self.w_customers[c_key]
                    
                    if not tableName in c: c[tableName] = [ ]
                    c[tableName].append(dict(map(lambda i: (columns[i], t[i]), cols)))
                    
                    ## Since ORDER_LINE doesn't have a C_ID, we have to store a reference to
                    ## this ORDERS record so that we can look it up later
                    if tableName == constants.TABLENAME_ORDERS:
                        o_key = (t[0], t[2], t[3]) # O_ID, O_D_ID, O_W_ID
                        self.w_orders[o_key] = (c_key, len(c[tableName])-1) # CUSTOMER, ORDER IDX
                ## FOR
            ## IF

        ## Otherwise just shove the tuples straight to the target collection
        else:
            for t in tuples:
                tuple_dicts.append(dict(map(lambda i: (columns[i], t[i]), num_columns)))
            ## FOR
            self.database[tableName].insert(tuple_dicts)
        ## IF
        
        return
        
    ## ----------------------------------------------
    ## loadFinishDistrict
    ## ----------------------------------------------
    def loadFinishDistrict(self, w_id, d_id):
        if self.denormalize:
            logging.debug("Pushing %d denormalized CUSTOMER records for WAREHOUSE %d DISTRICT %d into MongoDB" % (len(self.w_customers), w_id, d_id))
            self.database[constants.TABLENAME_CUSTOMER].insert(self.w_customers.values())
            self.w_customers.clear()
            self.w_orders.clear()
        ## IF

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Finished loading tables")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            for name in constants.ALL_TABLES:
                if self.denormalize and name in MongodbDriver.DENORMALIZED_TABLES[1:]: continue
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
            no = self.new_order.find_one({"NO_D_ID": d_id, "NO_W_ID": w_id}, {"NO_O_ID": 1})
            if no == None:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            assert len(no) > 0
            o_id = no["NO_O_ID"]
            
            if self.denormalize:
                ## getCId
                c = self.customer.find_one({"ORDERS.O_ID": o_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"C_ID": 1, "ORDERS.O_ID": 1, "ORDERS.ORDER_LINE": 1})
                assert c != None, "No customer record [O_ID=%d, D_ID=%d, W_ID=%d]" % (o_id, d_id, w_id)
                c_id = c["C_ID"]
                
                ## sumOLAmount + updateOrderLine
                ol_total = 0
                for o in c["ORDERS"]:
                    if o["O_ID"] == o_id:
                        orderLines = o["ORDER_LINE"]
                        for ol in orderLines:
                            ol_total += ol["OL_AMOUNT"]
                            ## We have to do this here because we can't update the nested array atomically
                            ol["OL_DELIVERY_D"] = ol_delivery_d
                        break
                ## FOR
                    
                if ol_total == 0:
                    pprint(params)
                    pprint(no)
                    pprint(c)
                    sys.exit(1)

                ## updateOrders + updateCustomer
                self.customer.update({"_id": c['_id'], "ORDERS.O_ID": o_id}, {"$set": {"ORDERS.$.O_CARRIER_ID": o_carrier_id, "ORDERS.$.ORDER_LINE": orderLines}, "$inc": {"C_BALANCE": ol_total}}, multi=False)

            else:
                ## getCId
                o = self.orders.find_one({"O_ID": o_id, "O_D_ID": d_id, "O_W_ID": w_id}, {"O_C_ID": 1})
                assert o != None
                c_id = o["O_C_ID"]
                
                ## sumOLAmount
                orderLines = self.order_line.find({"OL_O_ID": o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}, {"OL_AMOUNT": 1})
                assert orderLines != None
                ol_total = sum([ol["OL_AMOUNT"] for ol in orderLines])
                
                ## updateOrders
                self.orders.update(o, {"$set": {"O_CARRIER_ID": o_carrier_id}}, multi=False)
            
                ## updateOrderLine
                self.order_line.update({"OL_O_ID": o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}, {"$set": {"OL_DELIVERY_D": ol_delivery_d}}, multi=True)
                
                ## updateCustomer
                self.customer.update({"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"$inc": {"C_BALANCE": ol_total}})
            ## IF

            ## deleteNewOrder
            self.new_order.remove({"_id": no['_id']})

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            result.append((d_id, o_id))
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

        # createNewOrder
        self.new_order.insert({"NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id})

        o = {"O_ID": d_next_o_id, "O_ENTRY_D": o_entry_d, "O_CARRIER_ID": o_carrier_id, "O_OL_CNT": ol_cnt, "O_ALL_LOCAL": all_local}
        if self.denormalize:
            o[constants.TABLENAME_ORDER_LINE] = [ ]
        else:
            o["O_D_ID"] = d_id
            o["O_W_ID"] = w_id
            o["O_C_ID"] = c_id
            
            # createOrder
            self.orders.insert(o)  
            
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

            ol = {"OL_O_ID": d_next_o_id, "OL_NUMBER": ol_number, "OL_I_ID": ol_i_id, "OL_SUPPLY_W_ID": ol_supply_w_id, "OL_DELIVERY_D": o_entry_d, "OL_QUANTITY": ol_quantity, "OL_AMOUNT": ol_amount, "OL_DIST_INFO": s_dist_xx}

            if self.denormalize:
                # createOrderLine
                o[constants.TABLENAME_ORDER_LINE].append(ol)
            else:
                ol["OL_D_ID"] = d_id
                ol["OL_W_ID"] = w_id
                
                # createOrderLine
                self.order_line.insert(ol)
            ## IF

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR
        
        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        # createOrder
        self.customer.update({"_id": c["_id"]}, {"$push": {"ORDERS": o}})

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

        search_fields = {"C_W_ID": w_id, "C_D_ID": d_id}
        return_fields = {"C_ID": 1, "C_FIRST": 1, "C_MIDDLE": 1, "C_LAST": 1, "C_BALANCE": 1}
        if self.denormalize:
            for f in ['O_ID', 'O_CARRIER_ID', 'O_ENTRY_D']:
                return_fields["%s.%s" % (constants.TABLENAME_ORDERS, f)] = 1
            for f in ['OL_SUPPLY_W_ID', 'OL_I_ID', 'OL_QUANTITY']:
                return_fields["%s.%s.%s" % (constants.TABLENAME_ORDERS, constants.TABLENAME_ORDER_LINE, f)] = 1
        ## IF
        
        if c_id != None:
            # getCustomerByCustomerId
            search_fields["C_ID"] = c_id
            c = self.customer.find_one(search_fields, return_fields)
            assert c
            
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            search_fields['C_LAST'] = c_last
            
            all_customers = self.customer.find(search_fields, return_fields)
            namecnt = all_customers.count()
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        assert len(c) > 0
        assert c_id != None

        orderLines = [ ]
        order = None
        
        if self.denormalize:
            # getLastOrder
            if constants.TABLENAME_ORDERS in c:
                order = c[constants.TABLENAME_ORDERS][-1]
                # getOrderLines
                orderLines = order[constants.TABLENAME_ORDER_LINE]
        else:
            # getLastOrder
            order = self.orders.find({"O_W_ID": w_id, "O_D_ID": d_id, "O_C_ID": c_id}, {"O_ID": 1, "O_CARRIER_ID": 1, "O_ENTRY_D": 1}).sort("O_ID", direction=pymongo.DESCENDING).limit(1)[0]
            o_id = order["O_ID"]

            if order:
                # getOrderLines
                orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": o_id}, {"OL_SUPPLY_W_ID": 1, "OL_I_ID": 1, "OL_QUANTITY": 1, "OL_AMOUNT": 1, "OL_DELIVERY_D": 1})
        ## IF
            

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

        search_fields = {"C_W_ID": w_id, "C_D_ID": d_id}
        return_fields = {"C_BALANCE": 0, "C_YTD_PAYMENT": 0, "C_PAYMENT_CNT": 0}
        
        if c_id != None:
            # getCustomerByCustomerId
            search_fields["C_ID"] = c_id
            c = self.customer.find_one(search_fields, return_fields)
            assert c
            
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            search_fields['C_LAST'] = c_last
            all_customers = self.customer.find(search_fields, return_fields)
            namecnt = all_customers.count()
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        assert len(c) > 0
        assert c_id != None
        
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
        c_data = c["C_DATA"]

        # getWarehouse
        w = self.warehouse.find_one({"W_ID": w_id}, {"W_NAME": 1, "W_STREET_1": 1, "W_STREET_2": 1, "W_CITY": 1, "W_STATE": 1, "W_ZIP": 1})
        assert w
        
        # updateWarehouseBalance
        self.warehouse.update({"_id": w["_id"]}, {"$inc": {"H_AMOUNT": h_amount}})

        # getDistrict
        d = self.district.find_one({"D_W_ID": w_id, "D_ID": d_id}, {"D_NAME": 1, "D_STREET_1": 1, "D_STREET_2": 1, "D_CITY": 1, "D_STATE": 1, "D_ZIP": 1})
        assert d
        
        # updateDistrictBalance
        self.district.update({"_id": d["_id"]},  {"$inc": {"D_YTD": h_amount}})

        # Build CUSTOMER update command 
        customer_update = {"$inc": {"C_BALANCE": h_amount*-1, "C_YTD_PAYMENT": h_amount, "C_PAYMENT_CNT": 1}}
        
        # Customer Credit Information
        if c["C_CREDIT"] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            customer_update["$set"] = {"C_DATA": c_data}
        ## IF

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (w["W_NAME"], d["D_NAME"])
        h = {"H_D_ID": d_id, "H_W_ID": w_id, "H_DATE": h_date, "H_AMOUNT": h_amount, "H_DATA": h_data}
        if self.denormalize:
            # insertHistory + updateCustomer
            customer_update["$push"] = {constants.TABLENAME_HISTORY: h}
            self.customer.update({"_id": c["_id"]}, customer_update)
        else:
            # updateCustomer
            self.customer.update({"_id": c["_id"]}, customer_update)
            
            # insertHistory
            self.history.insert(h)

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
        if self.denormalize:
            c = self.customer.find({"C_W_ID": w_id, "C_D_ID": d_id, "ORDERS.O_ID": {"$lt": o_id, "$gte": o_id-20}}, {"ORDERS.ORDER_LINE.OL_I_ID": 1})
            assert c
            orderLines = [ ]
            for ol in c:
                assert "ORDER_LINE" in ol["ORDERS"][0]
                orderLines.extend(ol["ORDERS"][0]["ORDER_LINE"])
        else:
            orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": {"$lt": o_id, "$gte": o_id-20}}, {"OL_I_ID": 1})
            
        assert orderLines
        ol_ids = set()
        for ol in orderLines:
            ol_ids.add(ol["OL_I_ID"])
        ## FOR
        result = self.stock.find({"S_W_ID": w_id, "S_I_ID": {"$in": list(ol_ids)}, "S_QUANTITY": {"$lt": threshold}}).count()
        
        return int(result)
        
## CLASS