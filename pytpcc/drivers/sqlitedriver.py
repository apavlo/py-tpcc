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

from __future__ import with_statement

import os
import sqlite3
import logging
import commands

import constants
from abstractdriver import *

## ==============================================
## SqliteDriver
## ==============================================
class SqliteDriver(AbstractDriver):
    DEFAULT_CONFIG = [
        ( "database", "The path to the SQLite database", "/tmp/tpcc.db" ),
        ( "reset", "Reset the database", True )
    ]
    
    def __init__(self, ddl):
        super(SqliteDriver, self).__init__("sqlite", ddl)
        self.database = None
        self.conn = None
        self.cursor = None
    ## DEF
    
    def makeDefaultConfig(self):
        return SqliteDriver.DEFAULT_CONFIG
    ## DEF
    
    def loadConfig(self, config):
        for key in map(lambda x: x[0], SqliteDriver.DEFAULT_CONFIG):
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.database = config["database"]
        
        if config["reset"] and os.path.exists(self.database):
            logging.info("Deleting database '%s'" % self.database)
            os.unlink(self.database)
        
        if os.path.exists(self.database) == False:
            logging.info("Loading DDL file '%s'" % (self.ddl))
            ## HACK
            cmd = "sqlite3 %s < %s" % (self.database, self.ddl)
            (result, output) = commands.getstatusoutput(cmd)
            assert result == 0, cmd + "\n" + output
        ## IF
            
        self.conn = sqlite3.connect(self.database)
        self.cursor = self.conn.cursor()
    ## DEF
    
    def loadTuples(self, table, tuples):
        if len(tuples) == 0: return
        
        p = ["?"]*len(tuples[0])
        sql = "INSERT INTO %s VALUES (%s)" % (table, ",".join(p))
        self.cursor.executemany(sql, tuples)
        
        logging.info("Loaded %d tuples for table %s" % (len(tuples), table))
        return
    ## DEF
    
    def loadFinish(self):
        logging.info("Commiting changes to database")
        self.conn.commit()
    ## DEF
    
    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        getWarehouseTaxRate = "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?" # w_id
        getDistrict = "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?" # d_id, w_id
        incrementNextOrderId = "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?" # d_next_o_id, d_id, w_id
        getCustomer = "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?" # w_id, d_id, c_id
        createOrder = "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" # d_next_o_id, d_id, w_id, c_id, timestamp, o_carrier_id, o_ol_cnt, o_all_local
        createNewOrder = "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)" # o_id, d_id, w_id
        getItemInfo = "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?" # ol_i_id
        getStockInfo = "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?" # d_id, ol_i_id, ol_supply_w_id
        updateStock = "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?" # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        createOrderLine = "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
        
        for k in params.keys():
            locals()[k] = params[k]
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        ## Determine if this is an all local order or not
        all_local = True
        for i in range(len(i_ids)):
            all_local = all_local and i_w_ids[i] == w_id

        self.cursor.executemany(getItemInfo, i_ids)
        items = self.cursor.fetchall()
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
        self.cursor.execute(getWarehouseTaxRate, w_id)
        w_tax = self.cursor.fetchone()[0]
        
        self.cursor.execute(getDistrict, d_id, w_id)
        district_info = self.cursor.fetchone()
        d_tax = district_info[0]
        d_next_o_id = district_info[1]
        
        self.cursor.execute(getCustomer, w_id, d_id, c_id)
        customer_info = self.cursor.fetchone()
        c_discount = customer_info[0]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        
        self.cursor.execute(incrementNextOrderId, d_next_o_id + 1, d_id, w_id)
        self.cursor.execute(createOrder, d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local)
        self.cursor.execute(createNewOrder, d_next_o_id, d_id, w_id)

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        ol_input = [ (i_w_ids[i], i_ids[i]) for i in range(len(i_ids)) ]
        self.cursor.executemany(getStockInfo % (d_id), ol_input)
        stockresults = self.cursor.fetchall()
        assert len(stockresults) == len(i_ids)

        item_data = [ ]
        total = 0
        for i in range(len(i_ids)):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            assert len(stockresults[i]) > 1, "Cannot find stock info for item should not happen with valid database"
            itemInfo = items[i]
            stockInfo = stockresults[i]

            i_name = itemInfo[1]
            i_data = itemInfo[2]
            i_price = itemInfo[0]

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

            self.cursor.execute(updateStock, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id)

            if i_data.find(constants.ORIGINAL_BYTES) != -1 and s_data.find(constants.ORIGINAL_BYTES) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            self.cursor.execute(createOrderLine, d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx)

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR
        
        ## Commit!
        self.cursor.commit()

        ## Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        
        return [ customer_info, misc_info, item_data ]

    ## DEF
        
    
    def doStockLevel(self, params):
        getOId = "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?"
        getStockCount = """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = ?
              AND OL_D_ID = ?
              AND OL_O_ID < ?
              AND OL_O_ID >= ?
              AND S_W_ID = ?
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < ?
        """
        
        self.cursor.execute(getOId, params["w_id"], params["d_id"])
        result = self.cursor.fetchone()
        assert result
        o_id = result[0]
        
        self.cursor.execute(getStockCount, params["w_id"], params["d_id"], o_id, (o_id - 20), params["w_id"], params["threshold"])
        result = self.cursor.fetchone()
        return int(result[0])
    ## DEF
        
## CLASS

        
