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
import sqlite3
import logging
import commands
from pprint import pprint,pformat

import constants
from abstractdriver import *

TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", #
        "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
        "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
        "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
        "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
        "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        
    },
    
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id        
    },
    
    "PAYMENT": {
        
        "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
        "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", # h_amount, w_id
        "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
        "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?", # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    },
    
    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", 
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = ?
              AND OL_D_ID = ?
              AND OL_O_ID < ?
              AND OL_O_ID >= ?
              AND S_W_ID = ?
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < ?
        """,
    },
}


## ==============================================
## SqliteDriver
## ==============================================
class SqliteDriver(AbstractDriver):
    DEFAULT_CONFIG = [
        ( "database", "The path to the SQLite database", "/tmp/tpcc.db" ),
        ( "reset", "Reset the database", False )
    ]
    
    def __init__(self, ddl):
        super(SqliteDriver, self).__init__("sqlite", ddl)
        self.database = None
        self.conn = None
        self.cursor = None
    ## DEF
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return SqliteDriver.DEFAULT_CONFIG
    ## DEF
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in map(lambda x: x[0], SqliteDriver.DEFAULT_CONFIG):
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.database = config["database"]
        
        if config["reset"] and os.path.exists(self.database):
            logging.debug("Deleting database '%s'" % self.database)
            os.unlink(self.database)
        
        if os.path.exists(self.database) == False:
            logging.debug("Loading DDL file '%s'" % (self.ddl))
            ## HACK
            cmd = "sqlite3 %s < %s" % (self.database, self.ddl)
            (result, output) = commands.getstatusoutput(cmd)
            assert result == 0, cmd + "\n" + output
        ## IF
            
        self.conn = sqlite3.connect(self.database)
        self.cursor = self.conn.cursor()
    ## DEF
    
    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, table, tuples):
        if len(tuples) == 0: return
        
        p = ["?"]*len(tuples[0])
        sql = "INSERT INTO %s VALUES (%s)" % (table, ",".join(p))
        self.cursor.executemany(sql, tuples)
        
        logging.debug("Loaded %d tuples for table %s" % (len(tuples), table))
        return
    ## DEF

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Commiting changes to database")
        self.conn.commit()
    ## DEF

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        q = TXN_QUERIES["DELIVERY"]
        
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            self.cursor.execute(q["getNewOrder"], [d_id, w_id])
            newOrder = self.cursor.fetchone()
            if len(newOrder) == 0:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            assert len(newOrder) > 0
            no_o_id = newOrder[0]
            
            self.cursor.execute(q["getCId"], [no_o_id, d_id, w_id])
            c_id = self.cursor.fetchone()[0]
            
            self.cursor.execute(q["sumOLAmount"], [no_o_id, d_id, w_id])
            ol_total = self.cursor.fetchone()[0]

            self.cursor.execute(q["deleteNewOrder"], [d_id, w_id, no_o_id])
            self.cursor.execute(q["updateOrders"], [o_carrier_id, no_o_id, d_id, w_id])
            self.cursor.execute(q["updateOrderLine"], [ol_delivery_d, no_o_id, d_id, w_id])

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            self.cursor.execute(q["updateCustomer"], [ol_total, c_id, d_id, w_id])

            result.append((d_id, no_o_id))
        ## FOR

        self.conn.commit()
        return result
    ## DEF

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        q = TXN_QUERIES["NEW_ORDER"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True
        items = [ ]
        for i in range(len(i_ids)):
            ## Determine if this is an all local order or not
            all_local = all_local and i_w_ids[i] == w_id
            self.cursor.execute(q["getItemInfo"], [i_ids[i]])
            items.append(self.cursor.fetchone())
        assert len(items) == len(i_ids)
        
        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        for item in items:
            if len(item) == 0:
                ## TODO Abort here!
                return
        ## FOR
        
        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        self.cursor.execute(q["getWarehouseTaxRate"], [w_id])
        w_tax = self.cursor.fetchone()[0]
        
        self.cursor.execute(q["getDistrict"], [d_id, w_id])
        district_info = self.cursor.fetchone()
        d_tax = district_info[0]
        d_next_o_id = district_info[1]
        
        self.cursor.execute(q["getCustomer"], [w_id, d_id, c_id])
        customer_info = self.cursor.fetchone()
        c_discount = customer_info[0]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        
        self.cursor.execute(q["incrementNextOrderId"], [d_next_o_id + 1, d_id, w_id])
        self.cursor.execute(q["createOrder"], [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local])
        self.cursor.execute(q["createNewOrder"], [d_next_o_id, d_id, w_id])

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        item_data = [ ]
        total = 0
        for i in range(len(i_ids)):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            itemInfo = items[i]
            i_name = itemInfo[1]
            i_data = itemInfo[2]
            i_price = itemInfo[0]

            self.cursor.execute(q["getStockInfo"] % (d_id), [ol_i_id, ol_supply_w_id])
            stockInfo = self.cursor.fetchone()
            s_quantity = stockInfo[0]
            s_ytd = stockInfo[2]
            s_order_cnt = stockInfo[3]
            s_remote_cnt = stockInfo[4]
            s_data = stockInfo[1]
            s_dist_xx = stockInfo[5] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            
            if ol_supply_w_id != w_id: s_remote_cnt += 1

            self.cursor.execute(q["updateStock"], [s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id])

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            self.cursor.execute(q["createOrderLine"], [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx])

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR
        
        ## Commit!
        self.conn.commit()

        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        
        return [ customer_info, misc, item_data ]

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = TXN_QUERIES["ORDER_STATUS"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1)/2
            customer = all_customers[index]
            c_id = customer[0]
        assert len(customer) > 0
        assert c_id != None

        self.cursor.execute(q["getLastOrder"], [w_id, d_id, c_id])
        order = self.cursor.fetchone()
        if order:
            self.cursor.execute(q["getOrderLines"], [w_id, d_id, order[0]])
            orderLines = self.cursor.fetchone()
        else:
            orderLines = [ ]

        self.conn.commit()
        return [ customer, order, orderLines ]

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        q = TXN_QUERIES["PAYMENT"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1)/2
            customer = all_customers[index]
            c_id = customer[0]
        assert len(customer) > 0
        c_balance = customer[14] - h_amount
        c_ytd_payment = customer[15] + h_amount
        c_payment_cnt = customer[16] + 1
        c_data = customer[17]

        self.cursor.execute(q["getWarehouse"], [w_id])
        warehouse = self.cursor.fetchone()
        
        self.cursor.execute(q["getDistrict"], [w_id, d_id])
        district = self.cursor.fetchone()
        
        self.cursor.execute(q["updateWarehouseBalance"], [h_amount, w_id])
        self.cursor.execute(q["updateDistrictBalance"], [h_amount, w_id, d_id])

        # Customer Credit Information
        if customer[11] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            self.cursor.execute(q["updateBCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id])
        else:
            c_data = ""
            self.cursor.execute(q["updateGCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id])

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (warehouse[0], district[0])
        # Create the history record
        self.cursor.execute(q["insertHistory"], [c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data])

        self.conn.commit()

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return [ warehouse, district, customer ]
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        q = TXN_QUERIES["STOCK_LEVEL"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        self.cursor.execute(q["getOId"], [w_id, d_id])
        result = self.cursor.fetchone()
        assert result
        o_id = result[0]
        
        self.cursor.execute(q["getStockCount"], [w_id, d_id, o_id, (o_id - 20), w_id, threshold])
        result = self.cursor.fetchone()
        
        self.conn.commit()
        
        return int(result[0])
    ## DEF
        
## CLASS

        
