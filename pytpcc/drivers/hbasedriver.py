# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# HBase version:
# Copyright (C) 2011
# Zikai Wang
# galaxyngc1309@gmail.com
#
# Original SQLite version:
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
import uuid
import constants
from java.lang import Integer, Float, String
from org.apache.hadoop.hbase import HBaseConfiguration, HTableDescriptor, HColumnDescriptor
from org.apache.hadoop.hbase.client import HBaseAdmin, HTable, Put, Get, Scan, Delete, Result, ResultScanner
from org.apache.hadoop.hbase.util import Bytes
from org.apache.hadoop.hbase.filter import PrefixFilter
from abstractdriver import AbstractDriver
from pprint import pprint,pformat


## ==============================================
## HBase Tables Layout
## ==============================================
## TABLE WAREHOUSE
## W_ID W_NAME W_STREET_1 W_STREET_2 W_CITY W_STATE W_ZIP W_TAX W_YTD
## Row key: W_ID
## Column Family 1: W_NAME W_STREET_1 W_STREET_2 W_CITY W_STATE W_ZIP
## Column Family 2: W_TAX
## Column Family 3: W_YTD

## TABLE DISTRICT
## D_ID D_W_ID D_NAME D_STREET_1 D_STREET_2 D_CITY D_STATE_ D_ZIP D_TAX D_YTD D_NEXT_O_ID
## Row key: D_W_ID D_ID
## Column Family 1: D_TAX D_NEXT_O_ID
## Column Family 2: D_NAME D_STREET_1 D_STREET_2 D_CITY D_STATE D_ZIP
## Column Family 3: D_YTD

## TABLE CUSTOMER
## C_ID C_D_ID C_W_ID C_FIRST C_MIDDLE C_LAST C_STREET_1 C_STREET_2 C_CITY #C_STATE C_ZIP C_PHONE C_SINCE C_CREDIT C_CREDIT_LIM C_DISCOUNT C_BALANCE C_YTD_PAYMENT C_PAYMENT_CNT C_DELIVERY_CNT C_DATA
## Row key: C_W_ID C_D_ID C_ID
## Column Family 1: C_FIRST C_MIDDLE C_LAST C_STREET_1 C_STREET_2 C_CITY #C_STATE C_ZIP C_PHONE C_SINCE C_CREDIT C_CREDIT_LIM C_DISCOUNT C_BALANCE C_YTD_PAYMENT C_PAYMENT_CNT C_DATA
## Colmn Family 2: C_DELIVERY_CNT

## TABLE HISTORY
## H_C_ID H_C_D_ID H_C_W_ID H_D_ID H_W_ID H_DATE H_AMOUNT H_DATA
## Row key: UUID
## Column Family 1: H_C_ID H_C_D_ID H_C_W_ID H_D_ID H_W_ID H_DATE H_AMOUNT H_DATA

## TABLE NEW_ORDER
## NO_O_ID NO_D_ID NO_W_ID
## Row key: (NO_W_ID NO_D_ID NO_O_ID)
## Column Family 1: (NO_O_ID NO_D_ID NO_W_ID) 

## TABLE ORDERS
## O_ID O_D_ID O_W_ID O_C_ID O_ENTRY_D O_CARRIER_ID O_OL_CNT O_ALL_LOCAL
## Row key: O_W_ID O_D_ID O_ID
## Column Family 1: (O_C_ID O_ENTRY_D O_CARRIER_ID)
## Column Family 2: (O_OL_CNT O_ALL_LOCAL)

## TABLE ORDER_LINE
## OL_O_ID OL_D_ID OL_W_ID OL_NUMBER OL_I_ID OL_SUPPLY_W_ID OL_DELIVERY_D OL_QUANTITY OL_AMOUNT OL_DIST_INFO
## Row key: OL_W_ID OL_D_ID OL_O_ID OL_NUMBER 
## Column Family 1: OL_I_ID OL_SUPPLY_W_ID OL_DELIVERY_D OL_QUANTITY OL_AMOUNT 
## Column Family 2: OL_DIST_INFO

## TABLE ITEM
## I_ID I_IM_ID I_NAME I_PRICE I_DATA
## Row key: I_ID
## Column Family1: I_NAME I_PRICE I_DATA
## Column Family2: I_IM_ID

## TABLE STOCK
## S_I_ID S_W_ID S_QUANTITY S_DIST_01 S_DIST_02 S_DIST_03 S_DIST_04 S_DIST_05 S_DIST_06 S_DIST_07 S_DIST_08 S_DIST_09 S_DIST_10 S_YTD S_ORDER_CNT S_REMOTE_CNT S_DATA 
## Row key: S_W_ID S_I_ID
## Column Family1: S_QUANTITY S_DIST_01 S_DIST_02 S_DIST_03 S_DIST_04 S_DIST_05 S_DIST_06 S_DIST_07 S_DIST_08 S_DIST_09 S_DIST_10 S_YTD S_ORDER_CNT S_REMOTE_CNT S_DATA 

## Note that HBase cell values are uninterpreted bytes. Therefore, TPC-C values are serialized from their original types with org.apache.hadoop.hbase.util.Bytes package

## TPC-C tables
TABLES = ["WAREHOUSE", "DISTRICT", "CUSTOMER", "HISTORY", "NEW_ORDER", "ORDERS", "ORDER_LINE", "ITEM", "STOCK"]

## column qualifiers for all columns in TPC-C tables
COLUMN_NAME = {
    "W_ID" : Bytes.toBytes("a"),
    "W_NAME" : Bytes.toBytes("b"),
    "W_STREET_1" : Bytes.toBytes("c"),
    "W_STREET_2" : Bytes.toBytes("d"),
    "W_CITY" : Bytes.toBytes("e"),
    "W_STATE" : Bytes.toBytes("f"),
    "W_ZIP" : Bytes.toBytes("g"),
    "W_TAX" : Bytes.toBytes("h"),
    "W_YTD" : Bytes.toBytes("i"),
    "D_ID" : Bytes.toBytes("a"),
    "D_W_ID" : Bytes.toBytes("b"),
    "D_NAME" : Bytes.toBytes("c"),
    "D_STREET_1" : Bytes.toBytes("d"),
    "D_STREET_2" : Bytes.toBytes("e"),
    "D_CITY" : Bytes.toBytes("f"),
    "D_STATE" : Bytes.toBytes("g"),
    "D_ZIP" : Bytes.toBytes("h"),
    "D_TAX" : Bytes.toBytes("i"),
    "D_YTD" : Bytes.toBytes("j"),
    "D_NEXT_O_ID" : Bytes.toBytes("k"),
    "C_ID" : Bytes.toBytes("a"),
    "C_D_ID" : Bytes.toBytes("b"),
    "C_W_ID" : Bytes.toBytes("c"),
    "C_FIRST" : Bytes.toBytes("d"),
    "C_MIDDLE" : Bytes.toBytes("e"),
    "C_LAST" : Bytes.toBytes("f"),
    "C_STREET_1" : Bytes.toBytes("g"),
    "C_STREET_2" : Bytes.toBytes("h"),
    "C_CITY" : Bytes.toBytes("i"),
    "C_STATE" : Bytes.toBytes("j"),
    "C_ZIP" : Bytes.toBytes("k"),
    "C_PHONE" : Bytes.toBytes("l"),
    "C_SINCE" : Bytes.toBytes("m"),
    "C_CREDIT" : Bytes.toBytes("n"),
    "C_CREDIT_LIM" : Bytes.toBytes("o"),
    "C_DISCOUNT" : Bytes.toBytes("p"),
    "C_BALANCE" : Bytes.toBytes("q"),
    "C_YTD_PAYMENT" : Bytes.toBytes("r"),
    "C_PAYMENT_CNT" : Bytes.toBytes("s"),
    "C_DELIVERY_CNT" : Bytes.toBytes("t"),
    "C_DATA" : Bytes.toBytes("u"),
    "H_C_ID" : Bytes.toBytes("a"),
    "H_C_D_ID" : Bytes.toBytes("b"),
    "H_C_W_ID" : Bytes.toBytes("c"),
    "H_D_ID" : Bytes.toBytes("d"),
    "H_W_ID" : Bytes.toBytes("e"),
    "H_DATE" : Bytes.toBytes("f"),
    "H_AMOUNT" : Bytes.toBytes("g"),
    "H_DATA" : Bytes.toBytes("h"),
    "NO_O_ID" : Bytes.toBytes("a"),
    "NO_D_ID" : Bytes.toBytes("b"),
    "NO_W_ID" : Bytes.toBytes("c"),
    "O_ID" : Bytes.toBytes("a"),
    "O_D_ID" : Bytes.toBytes("b"),
    "O_W_ID" : Bytes.toBytes("c"),
    "O_C_ID" : Bytes.toBytes("d"),
    "O_ENTRY_D" : Bytes.toBytes("e"),
    "O_CARRIER_ID" : Bytes.toBytes("f"),
    "O_OL_CNT" : Bytes.toBytes("g"),
    "O_ALL_LOCAL" : Bytes.toBytes("h"),
    "OL_O_ID" : Bytes.toBytes("a"),
    "OL_D_ID" : Bytes.toBytes("b"),
    "OL_W_ID" : Bytes.toBytes("c"),
    "OL_NUMBER" : Bytes.toBytes("d"),
    "OL_I_ID" : Bytes.toBytes("e"),
    "OL_SUPPLY_W_ID" : Bytes.toBytes("f"),
    "OL_DELIVERY_D" : Bytes.toBytes("g"),
    "OL_QUANTITY" : Bytes.toBytes("h"),
    "OL_AMOUNT" : Bytes.toBytes("i"),
    "OL_DIST_INFO" : Bytes.toBytes("j"),
    "I_ID" : Bytes.toBytes("a"),
    "I_IM_ID" : Bytes.toBytes("b"),
    "I_NAME" : Bytes.toBytes("c"),
    "I_PRICE" : Bytes.toBytes("d"),
    "I_DATA" : Bytes.toBytes("e"),
    "S_I_ID" : Bytes.toBytes("a"), 
    "S_W_ID" : Bytes.toBytes("b"),
    "S_QUANTITY" : Bytes.toBytes("c"),
    "S_DIST_01" : Bytes.toBytes("d"),
    "S_DIST_02" : Bytes.toBytes("e"),
    "S_DIST_03" : Bytes.toBytes("f"),
    "S_DIST_04" : Bytes.toBytes("g"),
    "S_DIST_05" : Bytes.toBytes("h"),
    "S_DIST_06" : Bytes.toBytes("i"),
    "S_DIST_07" : Bytes.toBytes("j"),
    "S_DIST_08" : Bytes.toBytes("k"),
    "S_DIST_09" : Bytes.toBytes("l"),
    "S_DIST_10" : Bytes.toBytes("m"),
    "S_YTD" : Bytes.toBytes("n"),
    "S_ORDER_CNT" : Bytes.toBytes("o"),
    "S_REMOTE_CNT" : Bytes.toBytes("p"),
    "S_DATA" : Bytes.toBytes("q"),
}


## ==============================================
## Optimization Options
## ==============================================

## do not write to WAL for PUT 
WRITE_TO_WAL = False
## a scanner will pre-fetch 100 tuples at once
SCANNER_CACHING = 100
## turn off auto flushing for PUT
AUTOFLUSH = False
## in-mem column family 
DATA_IN_MEM = True

## ==============================================
## HBaseDriver
## ==============================================
class HbaseDriver(AbstractDriver):  
    DEFAULT_CONFIG = {}

    def __init__(self, ddl):
        super(HbaseDriver, self).__init__("hbase", ddl)
        self.config = None
        self.admin = None

        self.warehouse_tbl = None
        self.district_tbl = None
        self.customer_tbl = None
        self.history_tbl = None
        self.new_order_tbl = None
        self.order_tbl = None
        self.order_line_tbl = None
        self.item_tbl = None
        self.stock_tbl = None

        ## column families for all columns in TPC-C tables
        self.col_fam1 = Bytes.toBytes("1")
        self.col_fam2 = Bytes.toBytes("2")
        self.col_fam3 = Bytes.toBytes("3")

    def makeDefaultConfig(self):
        return HbaseDriver.DEFAULT_CONFIG

    def loadConfig(self, config):
        pass

    ## ==============================================
    ## loadStart
    ## ==============================================
    def loadStart(self):
        ## get HBase client 
        self.config = HBaseConfiguration.create()
        self.admin = HBaseAdmin(self.config)

        ## Drop tables if they already exist
        for table in TABLES:
            if self.admin.tableExists(table) and self.admin.isTableAvailable(table):
               if self.admin.isTableEnabled(table):
                            self.admin.disableTable(table)
               self.admin.deleteTable(table)
        ## FOR

        ## CREATE TABLE WAREHOUSE
        htd = HTableDescriptor("WAREHOUSE")
        hcd = HColumnDescriptor("1")
        hcd.setInMemory(DATA_IN_MEM)
        htd.addFamily(hcd)
        hcd = HColumnDescriptor("2")
        hcd.setInMemory(DATA_IN_MEM)
        htd.addFamily(hcd) 
        hcd = HColumnDescriptor("3")
        hcd.setInMemory(DATA_IN_MEM)
        htd.addFamily(hcd)
        self.admin.createTable(htd)

        ## CREATE TABLE DISTRICT
        htd = HTableDescriptor("DISTRICT")
        hcd = HColumnDescriptor("1")
        hcd.setInMemory(DATA_IN_MEM)
        htd.addFamily(hcd)
        hcd = HColumnDescriptor("2")
        hcd.setInMemory(DATA_IN_MEM)
        htd.addFamily(hcd)
        hcd = HColumnDescriptor("3")
        hcd.setInMemory(DATA_IN_MEM)
        htd.addFamily(hcd)
        self.admin.createTable(htd)

        ## CREATE TABLE CUSTOMER
        htd = HTableDescriptor("CUSTOMER")
        hcd = HColumnDescriptor("1")
        htd.addFamily(hcd)
        htd.addFamily(HColumnDescriptor("2"))
        self.admin.createTable(htd)

        ## CREATE TABLE HISTORY
        htd = HTableDescriptor("HISTORY")
        htd.addFamily(HColumnDescriptor("1"))
        self.admin.createTable(htd)

        ## CREATE TABLE NEW_ORDER
        htd = HTableDescriptor("NEW_ORDER")
        hcd = HColumnDescriptor("1")
        htd.addFamily(hcd)
        self.admin.createTable(htd)

        ## CREATE TABLE ORDERS
        htd = HTableDescriptor("ORDERS")
        hcd = HColumnDescriptor("1")
        htd.addFamily(hcd)
        htd.addFamily(HColumnDescriptor("2"))
        self.admin.createTable(htd)

        ## CREATE TABLE ORDER_LINE
        htd = HTableDescriptor("ORDER_LINE")
        hcd = HColumnDescriptor("1")
        htd.addFamily(hcd)
        htd.addFamily(HColumnDescriptor("2"))
        self.admin.createTable(htd)

        ## CREATE TABLE ITEM
        htd = HTableDescriptor("ITEM")
        htd.addFamily(HColumnDescriptor("1"))
        htd.addFamily(HColumnDescriptor("2"))
        self.admin.createTable(htd)

        ## CREATE TABLE STOCK
        htd = HTableDescriptor("STOCK")
        htd.addFamily(HColumnDescriptor("1"))
        self.admin.createTable(htd)

        ## get handlers to all tables
        self.warehouse_tbl = HTable(self.config, "WAREHOUSE")
        self.warehouse_tbl.setAutoFlush(AUTOFLUSH)
        self.district_tbl = HTable(self.config, "DISTRICT")
        self.district_tbl.setAutoFlush(AUTOFLUSH)
        self.customer_tbl = HTable(self.config, "CUSTOMER")
        self.customer_tbl.setAutoFlush(AUTOFLUSH)
        self.history_tbl = HTable(self.config, "HISTORY")
        self.history_tbl.setAutoFlush(AUTOFLUSH)
        self.new_order_tbl = HTable(self.config, "NEW_ORDER")
        self.new_order_tbl.setAutoFlush(AUTOFLUSH)
        self.order_tbl = HTable(self.config, "ORDERS")
        self.order_tbl.setAutoFlush(AUTOFLUSH)
        self.order_line_tbl = HTable(self.config, "ORDER_LINE")
        self.order_line_tbl.setAutoFlush(AUTOFLUSH)
        self.item_tbl = HTable(self.config, "ITEM")
        self.item_tbl.setAutoFlush(AUTOFLUSH)
        self.stock_tbl = HTable(self.config, "STOCK")	
        self.stock_tbl.setAutoFlush(AUTOFLUSH)

    ## ==============================================
    ## loadFinish
    ## ==============================================
    def loadFinish(self):
        ## close handlers to all tables
        self.warehouse_tbl.close()
        self.district_tbl.close()
        self.customer_tbl.close()
        self.history_tbl.close()
        self.new_order_tbl.close()
        self.order_tbl.close()
        self.order_line_tbl.close()
        self.item_tbl.close()
        self.stock_tbl.close()

    ## ==============================================
    ## loadTuples
    ## ==============================================
    def loadTuples(self, tableName, tuples):
        ## load TPC-C data
        if tableName == "WAREHOUSE":
                for tuplei in tuples:
			w_id = Bytes.toBytes(Integer(tuplei[0]))
			w_name = Bytes.toBytes(String(tuplei[1]))
                        w_street_1 = Bytes.toBytes(String(tuplei[2]))
                        w_street_2 = Bytes.toBytes(String(tuplei[3]))
                        w_city = Bytes.toBytes(String(tuplei[4]))
                        w_state = Bytes.toBytes(String(tuplei[5]))
         	        w_zip = Bytes.toBytes(String(tuplei[6]))
         	        w_tax = Bytes.toBytes(Float(tuplei[7]))
         	        w_ytd = Bytes.toBytes(Float(tuplei[8]))                      
                        
                        row_key = w_id
                        put = Put(row_key)
                        put.setWriteToWAL(WRITE_TO_WAL)
                        put.add(self.col_fam1, COLUMN_NAME["W_NAME"], w_name)
                        put.add(self.col_fam1, COLUMN_NAME["W_STREET_1"], w_street_1)
                        put.add(self.col_fam1, COLUMN_NAME["W_STREET_2"], w_street_2)
                        put.add(self.col_fam1, COLUMN_NAME["W_CITY"], w_city)
                        put.add(self.col_fam1, COLUMN_NAME["W_STATE"], w_state)
                        put.add(self.col_fam1, COLUMN_NAME["W_ZIP"], w_zip)
                        put.add(self.col_fam2, COLUMN_NAME["W_TAX"], w_tax)
                        put.add(self.col_fam3, COLUMN_NAME["W_YTD"], w_ytd)
                        self.warehouse_tbl.put(put)			
                ## FOR

        elif tableName == "DISTRICT": 
                for tuplei in tuples:
         	        d_id = Bytes.toBytes(Integer(tuplei[0]))
         	        d_w_id = Bytes.toBytes(Integer(tuplei[1]))
         	        d_name = Bytes.toBytes(String(tuplei[2]))
          	        d_street_1 = Bytes.toBytes(String(tuplei[3]))
          	        d_street_2 = Bytes.toBytes(String(tuplei[4]))
         		d_city = Bytes.toBytes(String(tuplei[5]))
           		d_state = Bytes.toBytes(String(tuplei[6]))
         		d_zip = Bytes.toBytes(String(tuplei[7]))
         		d_tax = Bytes.toBytes(Float(tuplei[8]))
                	d_ytd = Bytes.toBytes(Float(tuplei[9]))
                 	d_next_o_id = Bytes.toBytes(Integer(tuplei[10]))

                        row_key = d_w_id
			row_key.append(0)
			row_key.extend(d_id)
                        put = Put(row_key)
                        put.setWriteToWAL(WRITE_TO_WAL)
                        put.add(self.col_fam1, COLUMN_NAME["D_TAX"], d_tax)
                        put.add(self.col_fam1, COLUMN_NAME["D_NEXT_O_ID"], d_next_o_id)
                        put.add(self.col_fam2, COLUMN_NAME["D_NAME"], d_name)                        
                        put.add(self.col_fam2, COLUMN_NAME["D_STREET_1"], d_street_1)
                        put.add(self.col_fam2, COLUMN_NAME["D_STREET_2"], d_street_2)
                        put.add(self.col_fam2, COLUMN_NAME["D_CITY"], d_city)
                        put.add(self.col_fam2, COLUMN_NAME["D_STATE"], d_state)
                        put.add(self.col_fam2, COLUMN_NAME["D_ZIP"], d_zip)
                        put.add(self.col_fam3, COLUMN_NAME["D_YTD"], d_ytd)
                        self.district_tbl.put(put)
                ## FOR


        elif tableName == "CUSTOMER":
                for tuplei in tuples:
                         c_id = Bytes.toBytes(Integer(tuplei[0]))
                         c_d_id = Bytes.toBytes(Integer(tuplei[1]))
                         c_w_id = Bytes.toBytes(Integer(tuplei[2]))
                         c_first = Bytes.toBytes(String(tuplei[3]))
                         c_middle = Bytes.toBytes(String(tuplei[4]))
                         c_last = Bytes.toBytes(String(tuplei[5]))
                         c_street_1 = Bytes.toBytes(String(tuplei[6]))
                         c_street_2 = Bytes.toBytes(String(tuplei[7]))
                         c_city = Bytes.toBytes(String(tuplei[8]))
                         c_state = Bytes.toBytes(String(tuplei[9]))
                         c_zip = Bytes.toBytes(String(tuplei[10]))
                         c_phone = Bytes.toBytes(String(tuplei[11]))
                         c_since = Bytes.toBytes(String(str(tuplei[12])))
                         c_credit = Bytes.toBytes(String(tuplei[13]))
                         c_credit_lim = Bytes.toBytes(Float(tuplei[14]))
                         c_discount = Bytes.toBytes(Float(tuplei[15]))
                         c_balance = Bytes.toBytes(Float(tuplei[16]))
                         c_ytd_payment = Bytes.toBytes(Float(tuplei[17]))
                         c_payment_cnt = Bytes.toBytes(Integer(tuplei[18]))
                         c_delivery_cnt = Bytes.toBytes(Integer(tuplei[19]))
                         c_data = Bytes.toBytes(String(tuplei[20]))
 
                         row_key = c_w_id
                         row_key.append(0)
                         row_key.extend(c_d_id)
                         row_key.append(0)
                         row_key.extend(c_id)
                         put = Put(row_key)
                         put.setWriteToWAL(WRITE_TO_WAL)
                         put.add(self.col_fam1, COLUMN_NAME["C_FIRST"], c_first)
                         put.add(self.col_fam1, COLUMN_NAME["C_MIDDLE"], c_middle)
                         put.add(self.col_fam1, COLUMN_NAME["C_LAST"], c_last)
                         put.add(self.col_fam1, COLUMN_NAME["C_STREET_1"], c_street_1)
                         put.add(self.col_fam1, COLUMN_NAME["C_STREET_2"], c_street_2)
                         put.add(self.col_fam1, COLUMN_NAME["C_CITY"], c_city)
                         put.add(self.col_fam1, COLUMN_NAME["C_STATE"], c_state)
                         put.add(self.col_fam1, COLUMN_NAME["C_ZIP"], c_zip)
                         put.add(self.col_fam1, COLUMN_NAME["C_PHONE"], c_phone)
                         put.add(self.col_fam1, COLUMN_NAME["C_SINCE"], c_since)
                         put.add(self.col_fam1, COLUMN_NAME["C_CREDIT"], c_credit)
                         put.add(self.col_fam1, COLUMN_NAME["C_CREDIT_LIM"], c_credit_lim)
                         put.add(self.col_fam1, COLUMN_NAME["C_DISCOUNT"], c_discount)
                         put.add(self.col_fam1, COLUMN_NAME["C_BALANCE"], c_balance)
                         put.add(self.col_fam1, COLUMN_NAME["C_YTD_PAYMENT"], c_ytd_payment)
                         put.add(self.col_fam1, COLUMN_NAME["C_PAYMENT_CNT"], c_payment_cnt)
                         put.add(self.col_fam1, COLUMN_NAME["C_DATA"], c_data)
                         put.add(self.col_fam2, COLUMN_NAME["C_DELIVERY_CNT"], c_delivery_cnt)
                         self.customer_tbl.put(put)
                ## FOR

        elif tableName == "HISTORY":
                for tuplei in tuples:
                        h_c_id = Bytes.toBytes(Integer(tuplei[0]))
                        h_c_d_id = Bytes.toBytes(Integer(tuplei[1]))
                        h_c_w_id = Bytes.toBytes(Integer(tuplei[2]))
                        h_d_id = Bytes.toBytes(Integer(tuplei[3]))
                        h_w_id = Bytes.toBytes(Integer(tuplei[4]))
                        h_date = Bytes.toBytes(String(str(tuplei[5])))
                        h_amount = Bytes.toBytes(Float(tuplei[6]))
                        h_data = Bytes.toBytes(String(tuplei[7]))
                         
                        row_key = str(uuid.uuid1())
                        put = Put(row_key)
                        put.setWriteToWAL(WRITE_TO_WAL)
                        put.add(self.col_fam1, COLUMN_NAME["H_C_ID"], h_c_id)
                        put.add(self.col_fam1, COLUMN_NAME["H_C_D_ID"], h_c_d_id)
                        put.add(self.col_fam1, COLUMN_NAME["H_C_W_ID"], h_c_w_id)
                        put.add(self.col_fam1, COLUMN_NAME["H_D_ID"], h_d_id)
                        put.add(self.col_fam1, COLUMN_NAME["H_W_ID"], h_w_id)
                        put.add(self.col_fam1, COLUMN_NAME["H_DATE"], h_date)
                        put.add(self.col_fam1, COLUMN_NAME["H_AMOUNT"], h_amount)
                        put.add(self.col_fam1, COLUMN_NAME["H_DATA"], h_data)
                        self.history_tbl.put(put)
                ## FOR

        elif tableName == "NEW_ORDER":
                for tuplei in tuples:
                        no_o_id = Bytes.toBytes(Integer(tuplei[0]))
                        no_d_id = Bytes.toBytes(Integer(tuplei[1]))
                        no_w_id = Bytes.toBytes(Integer(tuplei[2]))

                        row_key = no_w_id
                        row_key.append(0)
                        row_key.extend(no_d_id)
                        row_key.append(0)
                        row_key.extend(no_o_id)
                        put = Put(row_key)
                        put.setWriteToWAL(WRITE_TO_WAL)
                        put.add(self.col_fam1, COLUMN_NAME["NO_O_ID"], no_o_id)
                        put.add(self.col_fam1, COLUMN_NAME["NO_D_ID"], no_d_id)
                        put.add(self.col_fam1, COLUMN_NAME["NO_W_ID"], no_w_id)
                        self.new_order_tbl.put(put)
                ## FOR

        elif tableName == "ORDERS":
                for tuplei in tuples:
                        o_id = Bytes.toBytes(Integer(tuplei[0]))
                        o_d_id = Bytes.toBytes(Integer(tuplei[2]))
                        o_w_id = Bytes.toBytes(Integer(tuplei[3]))
                        o_c_id = Bytes.toBytes(Integer(tuplei[1]))
                        o_entry_d = Bytes.toBytes(String(str(tuplei[4])))
                        o_carrier_id = Bytes.toBytes(String(str(tuplei[5])))
                        o_ol_cnt = Bytes.toBytes(Integer(tuplei[6]))
                        o_all_local = Bytes.toBytes(Integer(tuplei[7]))

                        row_key = o_w_id
                        row_key.append(0)
                        row_key.extend(o_d_id)
			row_key.append(0)
			row_key.extend(o_id)
                        put = Put(row_key)
                        put.setWriteToWAL(WRITE_TO_WAL)
                        put.add(self.col_fam1, COLUMN_NAME["O_C_ID"], o_c_id)
                        put.add(self.col_fam1, COLUMN_NAME["O_ENTRY_D"], o_entry_d)
                        put.add(self.col_fam1, COLUMN_NAME["O_CARRIER_ID"], o_carrier_id)
                        put.add(self.col_fam2, COLUMN_NAME["O_OL_CNT"], o_ol_cnt)
                        put.add(self.col_fam2, COLUMN_NAME["O_ALL_LOCAL"], o_all_local)
                        self.order_tbl.put(put)
                ## FOR

        elif tableName == "ORDER_LINE": 
                for tuplei in tuples:
                        ol_o_id= Bytes.toBytes(Integer(tuplei[0]))
                        ol_d_id = Bytes.toBytes(Integer(tuplei[1]))
                        ol_w_id = Bytes.toBytes(Integer(tuplei[2]))
                        ol_number = Bytes.toBytes(Integer(tuplei[3]))
                        ol_i_id  = Bytes.toBytes(Integer(tuplei[4]))
                        ol_supply_w_id = Bytes.toBytes(Integer(tuplei[5]))
                        ol_delivery_d = Bytes.toBytes(String(str(tuplei[6])))
                        ol_quantity = Bytes.toBytes(Integer(tuplei[7]))
                        ol_amount = Bytes.toBytes(Float(tuplei[8]))
                        ol_dist_info = Bytes.toBytes(String(tuplei[9]))

                        row_key = ol_w_id
                        row_key.append(0)
                        row_key.extend(ol_d_id)
			row_key.append(0)
			row_key.extend(ol_o_id)
                        row_key.append(0)
                        row_key.extend(ol_number)
                        put = Put(row_key)
                        put.setWriteToWAL(WRITE_TO_WAL)
                        put.add(self.col_fam1, COLUMN_NAME["OL_I_ID"], ol_i_id)
                        put.add(self.col_fam1, COLUMN_NAME["OL_SUPPLY_W_ID"], ol_supply_w_id)
                        put.add(self.col_fam1, COLUMN_NAME["OL_DELIVERY_D"], ol_delivery_d)
                        put.add(self.col_fam1, COLUMN_NAME["OL_QUANTITY"], ol_quantity)
                        put.add(self.col_fam1, COLUMN_NAME["OL_AMOUNT"], ol_amount)
                        put.add(self.col_fam2, COLUMN_NAME["OL_DIST_INFO"], ol_dist_info)
                        self.order_line_tbl.put(put)
                ## FOR

        elif tableName == "ITEM":
                for tuplei in tuples:
                        i_id = Bytes.toBytes(Integer(tuplei[0]))
                        i_im_id = Bytes.toBytes(Integer(tuplei[1]))
                        i_name = Bytes.toBytes(String(tuplei[2]))
                        i_price = Bytes.toBytes(Float(tuplei[3]))
                        i_data = Bytes.toBytes(String(tuplei[4]))

                        row_key = i_id
                        put = Put(row_key)
                        put.setWriteToWAL(WRITE_TO_WAL)
                        put.add(self.col_fam1, COLUMN_NAME["I_NAME"], i_name)
                        put.add(self.col_fam1, COLUMN_NAME["I_PRICE"], i_price)
                        put.add(self.col_fam1, COLUMN_NAME["I_DATA"], i_data)
                        put.add(self.col_fam2, COLUMN_NAME["I_IM_ID"], i_im_id)
                        self.item_tbl.put(put)
                ## FOR

        elif tableName == "STOCK": 
                for tuplei in tuples:
                        s_i_id = Bytes.toBytes(Integer(tuplei[0]))
                        s_w_id = Bytes.toBytes(Integer(tuplei[1]))
                        s_quantity = Bytes.toBytes(Integer(tuplei[2]))
                        s_dist_01 = Bytes.toBytes(String(tuplei[3]))
                        s_dist_02 = Bytes.toBytes(String(tuplei[4]))
                        s_dist_03 = Bytes.toBytes(String(tuplei[5]))
                        s_dist_04 = Bytes.toBytes(String(tuplei[6]))
                        s_dist_05 = Bytes.toBytes(String(tuplei[7]))
                        s_dist_06 = Bytes.toBytes(String(tuplei[8]))
                        s_dist_07 = Bytes.toBytes(String(tuplei[9]))
                        s_dist_08 = Bytes.toBytes(String(tuplei[10]))
                        s_dist_09 = Bytes.toBytes(String(tuplei[11]))
                        s_dist_10 = Bytes.toBytes(String(tuplei[12]))
                        s_ytd = Bytes.toBytes(Integer(tuplei[13]))
                        s_order_cnt = Bytes.toBytes(Integer(tuplei[14]))
                        s_remote_cnt = Bytes.toBytes(Integer(tuplei[15]))
                        s_data = Bytes.toBytes(String(tuplei[16]))

                        row_key = s_w_id
                        row_key.append(0)
                        row_key.extend(s_i_id)
                        put = Put(row_key)
                        put.setWriteToWAL(WRITE_TO_WAL)
                        put.add(self.col_fam1, COLUMN_NAME["S_QUANTITY"], s_quantity)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_01"], s_dist_01)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_02"], s_dist_02)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_03"], s_dist_03)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_04"], s_dist_04)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_05"], s_dist_05)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_06"], s_dist_06)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_07"], s_dist_07)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_08"], s_dist_08)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_09"], s_dist_09)
                        put.add(self.col_fam1, COLUMN_NAME["S_DIST_10"], s_dist_10)
                        put.add(self.col_fam1, COLUMN_NAME["S_YTD"], s_ytd)
                        put.add(self.col_fam1, COLUMN_NAME["S_ORDER_CNT"], s_order_cnt)
                        put.add(self.col_fam1, COLUMN_NAME["S_REMOTE_CNT"], s_remote_cnt)
                        put.add(self.col_fam1, COLUMN_NAME["S_DATA"], s_data)
                        self.stock_tbl.put(put)
                ## FOR

    ## ==============================================
    ## executeStart
    ## ==============================================
    def executeStart(self):
        ## get HBase client 
        self.config = HBaseConfiguration.create()
        self.admin = HBaseAdmin(self.config)

        ## get handlers to all tables
        self.warehouse_tbl = HTable(self.config, "WAREHOUSE")
        self.warehouse_tbl.setScannerCaching(SCANNER_CACHING)
        self.district_tbl = HTable(self.config, "DISTRICT")
        self.district_tbl.setScannerCaching(SCANNER_CACHING)
        self.customer_tbl = HTable(self.config, "CUSTOMER")
        self.customer_tbl.setScannerCaching(SCANNER_CACHING)
        self.history_tbl = HTable(self.config, "HISTORY")
        self.history_tbl.setScannerCaching(SCANNER_CACHING)
        self.new_order_tbl = HTable(self.config, "NEW_ORDER")
        self.new_order_tbl.setScannerCaching(SCANNER_CACHING)
        self.order_tbl = HTable(self.config, "ORDERS")
        self.order_tbl.setScannerCaching(SCANNER_CACHING)
        self.order_line_tbl = HTable(self.config, "ORDER_LINE")
        self.order_line_tbl.setScannerCaching(SCANNER_CACHING)
        self.item_tbl = HTable(self.config, "ITEM")
        self.item_tbl.setScannerCaching(SCANNER_CACHING)
        self.stock_tbl = HTable(self.config, "STOCK")	
        self.stock_tbl.setScannerCaching(SCANNER_CACHING)

    ## ==============================================
    ## executeFinish
    ## ==============================================
    def executeFinish(self):
        ## close handlers to all tables
        self.warehouse_tbl.close()
        self.district_tbl.close()
        self.customer_tbl.close()
        self.history_tbl.close()
        self.new_order_tbl.close()
        self.order_tbl.close()
        self.order_line_tbl.close()
        self.item_tbl.close()
        self.stock_tbl.close()

    ## ==============================================
    ## doDelivery
    ## ==============================================
    def doDelivery(self, params):
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            ## getNewOrder
            start_row = self.getRowKey([w_id, d_id])
            s = Scan(start_row, self.getLexNextRowKey(start_row))
            s.addColumn(self.col_fam1, COLUMN_NAME["NO_W_ID"])
            scanner = self.new_order_tbl.getScanner(s)
            newOrderRes = [ ] 
            for res in scanner:
                current_row = res.getRow()
                no_o_id = Bytes.toInt(current_row[10:14])
                if no_o_id > -1: 
                   newOrderRes.append(res)
            ## FOR
            scanner.close()

            if len(newOrderRes) == 0:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            assert len(newOrderRes) > 0
            newOrder = newOrderRes[0]
            no_o_id = Bytes.toInt(newOrder.getRow()[10:14])

            ## getCId
            row_key = self.getRowKey([w_id, d_id, no_o_id])
            get = Get(row_key)
            get.addColumn(self.col_fam1, COLUMN_NAME["O_C_ID"])
            res = self.order_tbl.get(get)
            c_id = Bytes.toInt(res.getValue(self.col_fam1, COLUMN_NAME["O_C_ID"]))

            ## sumOLAmount
            start_row = self.getRowKey([w_id, d_id, no_o_id])
            s = Scan(start_row, self.getLexNextRowKey(start_row))
            s.addColumn(self.col_fam1, COLUMN_NAME["OL_AMOUNT"])
            scanner = self.order_line_tbl.getScanner(s)
            ol_total = 0.0
            scanner_count = 0
            rows = [ ]
            for res in scanner:
                rows.append(res.getRow())
                scanner_count = scanner_count + 1
                ol_total = ol_total + Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["OL_AMOUNT"]))    
            ## FOR
            scanner.close()

            ## deleteNewOrder
            row_key = self.getRowKey([w_id, d_id, no_o_id])
            delete = Delete(row_key)
            self.new_order_tbl.delete(delete)

            ## updateOrders
            row_key = self.getRowKey([w_id, d_id, no_o_id])
            put = Put(row_key)
            put.setWriteToWAL(WRITE_TO_WAL)
            put.add(self.col_fam1, COLUMN_NAME["O_CARRIER_ID"], Bytes.toBytes(String(str(o_carrier_id))))
            self.order_tbl.put(put)
                  
            ## updateOrderLine
            ## get the rows to update from results of sumOLAmount
            for current_row in rows:
                put = Put(current_row)
                put.setWriteToWAL(WRITE_TO_WAL)
                put.add(self.col_fam1, COLUMN_NAME["OL_DELIVERY_D"], Bytes.toBytes(String(str(ol_delivery_d))))
                self.order_line_tbl.put(put)
            ## FOR

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            assert scanner_count>0
            assert ol_total > 0.0

            ## "updateCustomer"
            row_key = self.getRowKey([w_id, d_id, c_id])
            get = Get(row_key)
            get.addColumn(self.col_fam1, COLUMN_NAME["C_BALANCE"])
            res = self.customer_tbl.get(get)
            c_balance = Bytes.toFloat(res.getValue(self.col_fam1, COLUMN_NAME["C_BALANCE"])) + ol_total
            put = Put(row_key)
            put.setWriteToWAL(WRITE_TO_WAL)
            put.add(self.col_fam1, COLUMN_NAME["C_BALANCE"], Bytes.toBytes(Float(c_balance)))
            self.customer_tbl.put(put)
             
            result.append((d_id, no_o_id))
        ## FOR
        return result

    ## ==============================================
    ## doNewOrder
    ## ==============================================
    def doNewOrder(self, params):
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
            ## getItemInfo
            row_key = self.getRowKey([i_ids[i]])
            get = Get(row_key)
            get.addColumn(self.col_fam1, COLUMN_NAME["I_PRICE"])
            get.addColumn(self.col_fam1, COLUMN_NAME["I_NAME"])
            get.addColumn(self.col_fam1, COLUMN_NAME["I_DATA"])
            res = self.item_tbl.get(get)
            current_item = [ ] 
            if res.getRow() is not None:
               current_price = Bytes.toFloat(res.getValue(self.col_fam1, COLUMN_NAME["I_PRICE"]))
               byte_arr = res.getValue(self.col_fam1, COLUMN_NAME["I_NAME"])
               current_name = Bytes.toString(byte_arr, 0, len(byte_arr))
               byte_arr = res.getValue(self.col_fam1, COLUMN_NAME["I_DATA"])
               current_data = Bytes.toString(byte_arr, 0, len(byte_arr))
               current_item = [current_price, current_name, current_data]
            items.append(current_item)
        ## FOR
            
        assert len(items) == len(i_ids)

        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        for item in items:
            if len(item) == 0:
                return
        ## FOR

        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        ## getWarehouseTaxRate
        row_key = self.getRowKey([w_id])
        get = Get(row_key)
        get.addColumn(self.col_fam2, COLUMN_NAME["W_TAX"])
        res = self.warehouse_tbl.get(get)
        w_tax = Bytes.toFloat(res.getValue(self.col_fam2, COLUMN_NAME["W_TAX"]))

        ## getDistrict
        row_key = self.getRowKey([w_id, d_id])
        get = Get(row_key)
        get.addColumn(self.col_fam1, COLUMN_NAME["D_TAX"])
        get.addColumn(self.col_fam1, COLUMN_NAME["D_NEXT_O_ID"])
        res = self.district_tbl.get(get)
        d_tax = Bytes.toFloat(res.getValue(self.col_fam1, COLUMN_NAME["D_TAX"]))
        d_next_o_id = Bytes.toInt(res.getValue(self.col_fam1, COLUMN_NAME["D_NEXT_O_ID"]))

        ## getCustomer
        row_key = self.getRowKey([w_id, d_id, c_id])
        get = Get(row_key)
        get.addColumn(self.col_fam1, COLUMN_NAME["C_DISCOUNT"])
        get.addColumn(self.col_fam1, COLUMN_NAME["C_LAST"])
        get.addColumn(self.col_fam1, COLUMN_NAME["C_CREDIT"])
        res = self.customer_tbl.get(get)
        c_discount = Bytes.toFloat(res.getValue(self.col_fam1, COLUMN_NAME["C_DISCOUNT"]))
        byte_arr_1 = res.getValue(self.col_fam1, COLUMN_NAME["C_LAST"])
        byte_arr_2 = res.getValue(self.col_fam1, COLUMN_NAME["C_CREDIT"])
        customer_info = [c_discount, Bytes.toString(byte_arr_1, 0, len(byte_arr_1)), Bytes.toString(byte_arr_2, 0 ,len(byte_arr_2))]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID

        ## incrementNextOrderId
        row_key = self.getRowKey([w_id, d_id])
        put = Put(row_key)
        put.setWriteToWAL(WRITE_TO_WAL)
        put.add(self.col_fam1, COLUMN_NAME["D_NEXT_O_ID"], Bytes.toBytes(Integer(d_next_o_id+1)))
        self.district_tbl.put(put)

        ## createOrder
        row_key = self.getRowKey([w_id, d_id, d_next_o_id])
        put = Put(row_key)
        put.setWriteToWAL(WRITE_TO_WAL)
        put.add(self.col_fam1, COLUMN_NAME["O_C_ID"], Bytes.toBytes(Integer(c_id)))
        put.add(self.col_fam1, COLUMN_NAME["O_ENTRY_D"], Bytes.toBytes(String(str(o_entry_d))))
        put.add(self.col_fam1, COLUMN_NAME["O_CARRIER_ID"], Bytes.toBytes(String(str(o_carrier_id))))
        put.add(self.col_fam2, COLUMN_NAME["O_OL_CNT"], Bytes.toBytes(Integer(ol_cnt)))
        put.add(self.col_fam2, COLUMN_NAME["O_ALL_LOCAL"], Bytes.toBytes(Integer(all_local)))
        self.order_tbl.put(put)

        ## createNewOrder
        row_key = self.getRowKey([w_id, d_id, d_next_o_id]) 
        put = Put(row_key)
        put.setWriteToWAL(WRITE_TO_WAL)
        put.add(self.col_fam1, COLUMN_NAME["NO_O_ID"], Bytes.toBytes(Integer(d_next_o_id)))
        put.add(self.col_fam1, COLUMN_NAME["NO_D_ID"], Bytes.toBytes(Integer(d_id)))
        put.add(self.col_fam1, COLUMN_NAME["NO_W_ID"], Bytes.toBytes(Integer(w_id)))
        self.new_order_tbl.put(put)

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

            ## getStockInfo
            row_key = self.getRowKey([ol_supply_w_id, ol_i_id])
            get = Get(row_key)
            get.addColumn(self.col_fam1, COLUMN_NAME["S_QUANTITY"])
            get.addColumn(self.col_fam1, COLUMN_NAME["S_DATA"])
            get.addColumn(self.col_fam1, COLUMN_NAME["S_YTD"])
            get.addColumn(self.col_fam1, COLUMN_NAME["S_ORDER_CNT"])
            get.addColumn(self.col_fam1, COLUMN_NAME["S_REMOTE_CNT"])
            get.addColumn(self.col_fam1, COLUMN_NAME["S_DIST_"+("%02d" % d_id)])
            res = self.stock_tbl.get(get)                     
            s_quantity = Bytes.toInt(res.getValue(self.col_fam1, COLUMN_NAME["S_QUANTITY"]))
            s_ytd = Bytes.toInt(res.getValue(self.col_fam1, COLUMN_NAME["S_YTD"]))
            s_order_cnt = Bytes.toInt(res.getValue(self.col_fam1, COLUMN_NAME["S_ORDER_CNT"]))
            s_remote_cnt = Bytes.toInt(res.getValue(self.col_fam1, COLUMN_NAME["S_REMOTE_CNT"]))
            byte_arr = res.getValue(self.col_fam1, COLUMN_NAME["S_DATA"])
            s_data = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1, COLUMN_NAME["S_DIST_"+("%02d" % d_id)])
            s_dist_xx = Bytes.toString(byte_arr, 0, len(byte_arr)) # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1

            if ol_supply_w_id != w_id: s_remote_cnt += 1

            ## updateStock
            row_key = self.getRowKey([ol_supply_w_id, ol_i_id])
            put = Put(row_key)
            put.setWriteToWAL(WRITE_TO_WAL)
            put.add(self.col_fam1, COLUMN_NAME["S_QUANTITY"], Bytes.toBytes(Integer(s_quantity)))
            put.add(self.col_fam1, COLUMN_NAME["S_YTD"], Bytes.toBytes(Integer(s_ytd)))
            put.add(self.col_fam1, COLUMN_NAME["S_ORDER_CNT"], Bytes.toBytes(Integer(s_order_cnt)))
            put.add(self.col_fam1, COLUMN_NAME["S_REMOTE_CNT"], Bytes.toBytes(Integer(s_remote_cnt)))
            self.stock_tbl.put(put)      

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            ## createOrderLine
            row_key = self.getRowKey([w_id, d_id, d_next_o_id, ol_number])
            put = Put(row_key)
            put.setWriteToWAL(WRITE_TO_WAL)
            put.add(self.col_fam1, COLUMN_NAME["OL_I_ID"], Bytes.toBytes(Integer(ol_i_id)))
            put.add(self.col_fam1, COLUMN_NAME["OL_SUPPLY_W_ID"], Bytes.toBytes(Integer(ol_supply_w_id)))
            put.add(self.col_fam1, COLUMN_NAME["OL_DELIVERY_D"], Bytes.toBytes(String(str(o_entry_d))))
            put.add(self.col_fam1, COLUMN_NAME["OL_QUANTITY"], Bytes.toBytes(Integer(ol_quantity)))
            put.add(self.col_fam1, COLUMN_NAME["OL_AMOUNT"], Bytes.toBytes(Float(ol_amount)))
            put.add(self.col_fam1, COLUMN_NAME["OL_DIST_INFO"], Bytes.toBytes(String(s_dist_xx)))
            self.order_line_tbl.put(put)      

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR

        ## Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]

        return [ customer_info, misc, item_data ]

    ## ==============================================
    ## doOrderStatus
    ## ==============================================
    def doOrderStatus(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]

        assert w_id, pformat(params)
        assert d_id, pformat(params)

        customer = None
        if c_id != None:
            ## getCustomerByCustomerId
            row_key = self.getRowKey([w_id, d_id, c_id])
            get = Get(row_key)
            get.addColumn(self.col_fam1, COLUMN_NAME["C_FIRST"])
            get.addColumn(self.col_fam1, COLUMN_NAME["C_MIDDLE"])
            get.addColumn(self.col_fam1, COLUMN_NAME["C_LAST"])
            get.addColumn(self.col_fam1, COLUMN_NAME["C_BALANCE"])
            customer = self.customer_tbl.get(get)           

        else:
            ## getCustomersByLastName
            getCustomerRes = [ ] 
            start_row = self.getRowKey([w_id, d_id])
            s = Scan(start_row, self.getLexNextRowKey(start_row))
            s.addColumn(self.col_fam1, COLUMN_NAME["C_FIRST"])
            s.addColumn(self.col_fam1, COLUMN_NAME["C_MIDDLE"])
            s.addColumn(self.col_fam1, COLUMN_NAME["C_LAST"])
            s.addColumn(self.col_fam1, COLUMN_NAME["C_BALANCE"])
            scanner = self.customer_tbl.getScanner(s)
            for res in scanner:
                byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_LAST"])
                current_c_last = Bytes.toString(byte_arr, 0, len(byte_arr))
                if current_c_last == c_last:
                   getCustomerRes.append(res)
            ## FOR
            scanner.close()
            
            assert len(getCustomerRes) > 0
            namecnt = len(getCustomerRes)

            getCustomerRes.sort(self.compareCustomerByFirst)
 
            index = (namecnt-1)/2
            customer = getCustomerRes[index]
            c_id = Bytes.toInt(customer.getRow()[10:14])
            
        assert customer != None
        assert c_id != None

        byte_arr = customer.getValue(self.col_fam1,COLUMN_NAME["C_FIRST"])
        customer_first = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = customer.getValue(self.col_fam1,COLUMN_NAME["C_MIDDLE"])
        customer_middle = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = customer.getValue(self.col_fam1,COLUMN_NAME["C_LAST"])
        customer_last = Bytes.toString(byte_arr, 0, len(byte_arr))
        customer_balance = Bytes.toFloat(customer.getValue(self.col_fam1,COLUMN_NAME["C_BALANCE"]))
        customer = [c_id, customer_first, customer_middle, customer_last, customer_balance]

        ## getLastOrder
        getOrderRes = [ ]
        start_row = self.getRowKey([w_id, d_id])
        s = Scan(start_row, self.getLexNextRowKey(start_row))
        s.addColumn(self.col_fam1, COLUMN_NAME["O_CARRIER_ID"])
        s.addColumn(self.col_fam1, COLUMN_NAME["O_ENTRY_D"])
        s.addColumn(self.col_fam1, COLUMN_NAME["O_C_ID"])
        scanner = self.order_tbl.getScanner(s)
        for res in scanner:
                current_o_c_id = Bytes.toInt(res.getValue(self.col_fam1,COLUMN_NAME["O_C_ID"]))
                if current_o_c_id == c_id:
                   getOrderRes.append(res)
        ## FOR
        scanner.close()

        getOrderRes.sort(self.compareOrderByOID)
        order = None
        if len(getOrderRes)>0 :        
           order = getOrderRes[0] 
        
        orderLines = [ ] 
        if order != None:
            order_o_id = Bytes.toInt(order.getRow()[10:14])
            byte_arr = order.getValue(self.col_fam1,COLUMN_NAME["O_CARRIER_ID"])
            order_o_carrier_id = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = order.getValue(self.col_fam1,COLUMN_NAME["O_ENTRY_D"])
            order_o_entry_d = Bytes.toString(byte_arr, 0, len(byte_arr))
            order = [order_o_id, order_o_carrier_id, order_o_entry_d]
            ## getOrderLines
            start_row = self.getRowKey([w_id, d_id, order_o_id])
            s = Scan(start_row, self.getLexNextRowKey(start_row))
            s.addColumn(self.col_fam1, COLUMN_NAME["OL_SUPPLY_W_ID"])
            s.addColumn(self.col_fam1, COLUMN_NAME["OL_I_ID"])
            s.addColumn(self.col_fam1, COLUMN_NAME["OL_QUANTITY"])
            s.addColumn(self.col_fam1, COLUMN_NAME["OL_AMOUNT"])
            s.addColumn(self.col_fam1, COLUMN_NAME["OL_DELIVERY_D"])
            scanner = self.order_line_tbl.getScanner(s)
            for res in scanner:
                current_ol_supply_w_id =  Bytes.toInt(res.getValue(self.col_fam1,COLUMN_NAME["OL_SUPPLY_W_ID"]))
                current_ol_i_id = Bytes.toInt(res.getValue(self.col_fam1,COLUMN_NAME["OL_I_ID"]))
                current_ol_quantity = Bytes.toInt(res.getValue(self.col_fam1,COLUMN_NAME["OL_QUANTITY"]))
                current_ol_amount = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["OL_AMOUNT"]))
                byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["OL_DELIVERY_D"])
                current_ol_delivery_d = Bytes.toString(byte_arr, 0, len(byte_arr))
                current_order_line = [current_ol_supply_w_id, current_ol_i_id, current_ol_quantity, current_ol_amount, current_ol_delivery_d]
                orderLines.append(current_order_line)
            ## FOR 
            scanner.close()

        else:
            orderLines = [ ]

        return [ customer, order, orderLines ]
        
    ## ==============================================
    ## doPayment
    ## ==============================================

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
            ## getCustomerByCustomerId
            row_key = self.getRowKey([w_id, d_id, c_id])
            get = Get(row_key)
            get.addFamily(self.col_fam1)
            res = self.customer_tbl.get(get)
            current_c_id = Bytes.toInt(res.getRow()[10:14])
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_FIRST"])
            current_c_first = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_MIDDLE"])
            current_c_middle = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_LAST"])
            current_c_last = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_STREET_1"])
            current_c_street_1 = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_STREET_2"])
            current_c_street_2 = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_CITY"])
            current_c_city = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_STATE"])
            current_c_state = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_ZIP"])
            current_c_zip = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_PHONE"])
            current_c_phone = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_SINCE"])
            current_c_since = Bytes.toString(byte_arr, 0, len(byte_arr))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_CREDIT"])
            current_c_credit = Bytes.toString(byte_arr, 0, len(byte_arr))
            current_c_credit_lim = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["C_CREDIT_LIM"]))
            current_c_discount = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["C_DISCOUNT"]))
            current_c_balance = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["C_BALANCE"]))
            current_c_ytd_payment = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["C_YTD_PAYMENT"]))
            current_c_payment_cnt = Bytes.toInt(res.getValue(self.col_fam1,COLUMN_NAME["C_PAYMENT_CNT"]))
            byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_DATA"])
            current_c_data = Bytes.toString(byte_arr, 0, len(byte_arr))
            customer = [current_c_id, current_c_first, current_c_middle, current_c_last,  current_c_street_1, current_c_street_2, current_c_city, current_c_state, current_c_zip, current_c_phone, current_c_since, current_c_credit, current_c_credit_lim, current_c_discount, current_c_balance, current_c_ytd_payment, current_c_payment_cnt, current_c_data] 
        else:
            ## getCustomersByLastName
            start_row = self.getRowKey([w_id, d_id])
            s = Scan(start_row, self.getLexNextRowKey(start_row))
            s.addFamily(self.col_fam1)
            scanner = self.customer_tbl.getScanner(s)
            all_customers = [ ]
            for res in scanner:
                byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_LAST"])
                current_c_last = Bytes.toString(byte_arr, 0, len(byte_arr))
                if current_c_last == c_last:
		   current_c_id = Bytes.toInt(res.getRow()[10:14])
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_FIRST"])
                   current_c_first = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_MIDDLE"])
                   current_c_middle = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_LAST"])
                   current_c_last = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_STREET_1"])
                   current_c_street_1 = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_STREET_2"])
                   current_c_street_2 = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_CITY"])
                   current_c_city = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_STATE"])
                   current_c_state = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_ZIP"])
                   current_c_zip = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_PHONE"])
                   current_c_phone = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_SINCE"])
                   current_c_since = Bytes.toString(byte_arr, 0, len(byte_arr))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_CREDIT"])
                   current_c_credit = Bytes.toString(byte_arr, 0, len(byte_arr))
                   current_c_credit_lim = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["C_CREDIT_LIM"]))
                   current_c_discount = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["C_DISCOUNT"]))
                   current_c_balance = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["C_BALANCE"]))
                   current_c_ytd_payment = Bytes.toFloat(res.getValue(self.col_fam1,COLUMN_NAME["C_YTD_PAYMENT"]))
                   current_c_payment_cnt = Bytes.toInt(res.getValue(self.col_fam1,COLUMN_NAME["C_PAYMENT_CNT"]))
                   byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["C_DATA"])
                   current_c_data = Bytes.toString(byte_arr, 0, len(byte_arr))

                   current_customer = [current_c_id, current_c_first, current_c_middle, current_c_last,  current_c_street_1, current_c_street_2, current_c_city, current_c_state, current_c_zip, current_c_phone, current_c_since, current_c_credit, current_c_credit_lim, current_c_discount, current_c_balance, current_c_ytd_payment, current_c_payment_cnt, current_c_data] 
                   all_customers.append(current_customer)
            ## FOR
            scanner.close()
            
            all_customers.sort(self.compareCustomerArrByFirst)
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

        ## getWarehouse
        row_key = self.getRowKey([w_id])
        get = Get(row_key)
        get.addFamily(self.col_fam1)
        get.addFamily(self.col_fam3)
        res = self.warehouse_tbl.get(get)
        byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["W_NAME"])
        current_w_name = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["W_STREET_1"])
        current_w_street_1 = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["W_STREET_2"])
        current_w_street_2 = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["W_CITY"])
        current_w_city = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["W_STATE"])
        current_w_state = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam1,COLUMN_NAME["W_ZIP"])
        current_w_zip = Bytes.toString(byte_arr, 0, len(byte_arr))
        ## pre-fetch w_ytd for updateWarehouseBalance
        current_w_ytd = Bytes.toFloat(res.getValue(self.col_fam3,COLUMN_NAME["W_YTD"]))
        warehouse = [current_w_name, current_w_street_1, current_w_street_2, current_w_city, current_w_state, current_w_zip]

        ## getDistrict
        row_key = self.getRowKey([w_id, d_id])
        get = Get(row_key)
        get.addFamily(self.col_fam2)
        get.addFamily(self.col_fam3)
        res = self.district_tbl.get(get)
        byte_arr = res.getValue(self.col_fam2,COLUMN_NAME["D_NAME"])
        current_d_name = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam2,COLUMN_NAME["D_STREET_1"])
        current_d_street_1 = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam2,COLUMN_NAME["D_STREET_2"])
        current_d_street_2 = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam2,COLUMN_NAME["D_CITY"])
        current_d_city = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam2,COLUMN_NAME["D_STATE"])
        current_d_state = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = res.getValue(self.col_fam2,COLUMN_NAME["D_ZIP"])
        current_d_zip = Bytes.toString(byte_arr, 0, len(byte_arr))
        ## pre-fetch d_ytd for updateDistrictBalance
        current_d_ytd = Bytes.toFloat(res.getValue(self.col_fam3,COLUMN_NAME["D_YTD"]))
        district = [current_d_name, current_d_street_1, current_d_street_2, current_d_city, current_d_state, current_d_zip]
        
        ## updateWarehouseBalance
        row_key = self.getRowKey([w_id])
        put = Put(row_key)
        put.setWriteToWAL(WRITE_TO_WAL)
        put.add(self.col_fam3, COLUMN_NAME["W_YTD"], Bytes.toBytes(Float(current_w_ytd + h_amount)))
        self.warehouse_tbl.put(put)      
        
        ## updateDistrictBalance
        row_key = self.getRowKey([w_id, d_id])
        put = Put(row_key)
        put.setWriteToWAL(WRITE_TO_WAL)
        put.add(self.col_fam3, COLUMN_NAME["D_YTD"], Bytes.toBytes(Float(current_d_ytd + h_amount)))
        self.district_tbl.put(put)      
        
        ## Customer Credit Information
        if customer[11] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            ## updateBCCustomer
            row_key = self.getRowKey([c_w_id, c_d_id, c_id])
            put = Put(row_key)
            put.setWriteToWAL(WRITE_TO_WAL)
            put.add(self.col_fam1, COLUMN_NAME["C_BALANCE"], Bytes.toBytes(Float(c_balance)))
            put.add(self.col_fam1, COLUMN_NAME["C_YTD_PAYMENT"], Bytes.toBytes(Float(c_ytd_payment)))
            put.add(self.col_fam1, COLUMN_NAME["C_PAYMENT_CNT"], Bytes.toBytes(Integer(c_payment_cnt)))
            put.add(self.col_fam1, COLUMN_NAME["C_DATA"], Bytes.toBytes(String(c_data)))
            self.customer_tbl.put(put)      
        else:
            c_data = ""
            ## updateGCCustomer
            row_key = self.getRowKey([c_w_id, c_d_id, c_id])
            put = Put(row_key)
            put.setWriteToWAL(WRITE_TO_WAL)
            put.add(self.col_fam1, COLUMN_NAME["C_BALANCE"], Bytes.toBytes(Float(c_balance)))
            put.add(self.col_fam1, COLUMN_NAME["C_YTD_PAYMENT"], Bytes.toBytes(Float(c_ytd_payment)))
            put.add(self.col_fam1, COLUMN_NAME["C_PAYMENT_CNT"], Bytes.toBytes(Integer(c_payment_cnt)))
            self.customer_tbl.put(put)      

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (warehouse[0], district[0])
        # Create the history record
        ## insertHistory
        row_key = Bytes.toBytes(str(uuid.uuid1()))
        put = Put(row_key)
        put.setWriteToWAL(WRITE_TO_WAL)
        put.add(self.col_fam1, COLUMN_NAME["H_C_ID"], Bytes.toBytes(Integer(c_id)))
        put.add(self.col_fam1, COLUMN_NAME["H_C_D_ID"], Bytes.toBytes(Integer(c_d_id)))
        put.add(self.col_fam1, COLUMN_NAME["H_C_W_ID"], Bytes.toBytes(Integer(c_w_id)))
        put.add(self.col_fam1, COLUMN_NAME["H_D_ID"], Bytes.toBytes(Integer(d_id)))
        put.add(self.col_fam1, COLUMN_NAME["H_W_ID"], Bytes.toBytes(Integer(w_id)))
        put.add(self.col_fam1, COLUMN_NAME["H_DATE"], Bytes.toBytes(String(str(h_date))))
        put.add(self.col_fam1, COLUMN_NAME["H_AMOUNT"], Bytes.toBytes(Float(h_amount)))
        put.add(self.col_fam1, COLUMN_NAME["H_DATA"], Bytes.toBytes(String(h_data)))
        self.history_tbl.put(put)        

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return [ warehouse, district, customer ]

    ## ==============================================
    ## doStockLevel
    ## ==============================================
    def doStockLevel(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

        ## getOId
        row_key = self.getRowKey([w_id, d_id])
        get = Get(row_key)
        get.addColumn(self.col_fam1, COLUMN_NAME["D_NEXT_O_ID"])
        res = self.district_tbl.get(get)
        assert res.getRow() is not None
        o_id = Bytes.toInt(res.getValue(self.col_fam1, COLUMN_NAME["D_NEXT_O_ID"]))

        ## getStockCount
        ol_i_id_set = set()
        start_row = self.getRowKey([w_id, d_id])
        s = Scan(start_row, self.getLexNextRowKey(start_row))
        s.addColumn(self.col_fam1, COLUMN_NAME["OL_I_ID"])
        scanner = self.order_line_tbl.getScanner(s)
        for res in scanner:
            current_row = res.getRow()
            ol_o_id = Bytes.toInt(current_row[10:14])
            if (ol_o_id < o_id) and (ol_o_id >= o_id - 20):
               current_ol_i_id = Bytes.toInt(res.getValue(self.col_fam1, COLUMN_NAME["OL_I_ID"]))
               row_key = self.getRowKey([w_id, current_ol_i_id])
               get = Get(row_key)
               get.addColumn(self.col_fam1, COLUMN_NAME["S_QUANTITY"])
               resx = self.stock_tbl.get(get)
               if resx.getRow() is not None:
                  current_s_quantity = Bytes.toInt(resx.getValue(self.col_fam1, COLUMN_NAME["S_QUANTITY"]))
                  if current_s_quantity < threshold:
                     ol_i_id_set.add(current_ol_i_id)
        ## FOR
        scanner.close()

        return len(ol_i_id_set)

    ## get a row key which is composite of primary keys
    def getRowKey(self,keylist):
        byte_arr = [ ]
        length = len(keylist)
        for i in range(length):
            byte_arr.extend(Bytes.toBytes(Integer(keylist[i])))
            if i<length-1:
               byte_arr.append(0)
        ## FOR
        return byte_arr                

    ## get the row key that is lexographical successor to argument according to byte order:
    def getLexNextRowKey(self, i):
        next = list(i)
        length = len(next)
        for j in range(length-1,0,-1):
            if (next[j] >= 0 and next[j] < 127) or (next[j] >= -128 and next[j] < -1):
               next[j] = next[j] + 1
               break
            elif next[j] == 127:
               next[j] = -128
               break
            elif next[j] == -1:
               next[j] = 0
        ## FOR
        return next
               
         
    ## compare function used in OrderStatus transaction: ASCE
    def compareCustomerByFirst(self, i, j):
        byte_arr = i.getValue(self.col_fam1,COLUMN_NAME["C_FIRST"])
        first_i = Bytes.toString(byte_arr, 0, len(byte_arr))
        byte_arr = j.getValue(self.col_fam1,COLUMN_NAME["C_FIRST"])
        first_j = Bytes.toString(byte_arr, 0, len(byte_arr))
        return cmp(first_i,first_j)

    ## compare function used in OrderStatus transaction: DESC
    def compareOrderByOID(self, i, j):
        oid_i = Bytes.toInt(i.getRow()[10:14])
        oid_j = Bytes.toInt(j.getRow()[10:14])
        return -cmp(oid_i,oid_j)

    ## compare function used in Payment transaction: ASCE
    def compareCustomerArrByFirst(self, i, j):
        first_i = i[1]
        first_j = i[1]
        return cmp(first_i,first_j)
