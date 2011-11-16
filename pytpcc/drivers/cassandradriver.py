# -*- coding: utf-8 -*-
# copyright (C) 2011
# Jingxin Feng, Xiaowei Wang
# jxfeng@cs.brown.edu
# xiaowei@cs.brown.edu
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


import pycassa
from pycassa.index import *
from pycassa.system_manager import *

import os
import logging
import commands
import uuid
from pprint import pprint,pformat
import constants

from abstractdriver import *
## ==============================================
## AbstractDriver
## ==============================================
class CassandraDriver(AbstractDriver):

    DEFAULT_CONFIG = {
        "hostname": ("The host address to the Cassandra database","localhost"),
        "port": ("Port number",9160),
        "name": ("Name","tpcc"),
        "keyspace":("Keyspace", "Keyspace1"),
	"replicationfactor": ("ReplicationFactor", 1)
    }



    def __init__(self, ddl):
        super(CassandraDriver,self).__init__("cassandra",ddl)
        self.conn = None
        self.name = "cassandra"
        self.database = None

        self.new_ordercf= None
        self.orderscf= None
        self.order_linecf= None
        self.customercf = None
        self.warehousecf = None
        self.districtcf = None
        self.historycf = None
        self.stockcf = None
        self.itemcf = None
    def makeDefaultConfig(self):
        return CassandraDriver.DEFAULT_CONFIG

    def loadConfig(self,config):
	for key in CassandraDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)        
        
         
        connection =  str(config["hostname"]+':'+str(config["port"]))
        
                
        keyspace = str(config["keyspace"])
        self.sys = SystemManager(connection)
        keyspaces = self.sys.list_keyspaces()
        fl = 0
        for i in range(len(keyspaces)):
            if str(keyspaces[i]) == keyspace:
               fl = 1
               break
        if fl == 0:     
            self.sys.create_keyspace(keyspace, SIMPLE_STRATEGY,{'replication_factor' : str(config["replicationfactor"])})
            self.sys.create_column_family(keyspace, 'NEW_ORDER', comparator_type = UTF8_TYPE)
            self.sys.create_column_family(keyspace, 'ORDERS', comparator_type = UTF8_TYPE)
            self.sys.create_column_family(keyspace, 'ORDER_LINE', comparator_type = UTF8_TYPE)
            self.sys.create_column_family(keyspace, 'CUSTOMER', comparator_type = UTF8_TYPE)
            self.sys.create_column_family(keyspace, 'WAREHOUSE', comparator_type = UTF8_TYPE)
            self.sys.create_column_family(keyspace, 'DISTRICT', comparator_type = UTF8_TYPE)
            self.sys.create_column_family(keyspace, 'HISTORY', comparator_type = UTF8_TYPE)
            self.sys.create_column_family(keyspace, 'STOCK', comparator_type = UTF8_TYPE)
            self.sys.create_column_family(keyspace, 'ITEM', comparator_type = UTF8_TYPE)
        

            self.sys.alter_column(keyspace,'WAREHOUSE','W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'WAREHOUSE','W_NAME',UTF8_TYPE)
            self.sys.alter_column(keyspace,'WAREHOUSE','W_STREET_1',UTF8_TYPE)
            self.sys.alter_column(keyspace,'WAREHOUSE','W_STREET_2',UTF8_TYPE)
            self.sys.alter_column(keyspace,'WAREHOUSE','W_CITY',UTF8_TYPE)
            self.sys.alter_column(keyspace,'WAREHOUSE','W_STATE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'WAREHOUSE','W_ZIP',UTF8_TYPE)
            self.sys.alter_column(keyspace,'WAREHOUSE','W_TAX',UTF8_TYPE)
            self.sys.alter_column(keyspace,'WAREHOUSE','W_YTD',UTF8_TYPE)
        
        
            self.sys.alter_column(keyspace,'DISTRICT','D_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_NAME',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_STREET_1',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_STREET_2',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_CITY',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_STATE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_ZIP',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_TAX',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_YTD',UTF8_TYPE)
            self.sys.alter_column(keyspace,'DISTRICT','D_NEXT_O_ID',UTF8_TYPE)

        
            self.sys.alter_column(keyspace,'CUSTOMER','C_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_D_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_FIRST',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_MIDDLE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_LAST',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_STREET_1',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_STREET_2',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_CITY',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_STATE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_ZIP',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_PHONE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_SINCE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_CREDIT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_CREDIT_LIM',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_DISCOUNT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_BALANCE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_YTD_PAYMENT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_PAYMENT_CNT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_DELIVERY_CNT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'CUSTOMER','C_DATA',UTF8_TYPE)
        
        
        
        
        

        
        
            self.sys.alter_column(keyspace,'HISTORY','H_C_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'HISTORY','H_C_D_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'HISTORY','H_C_W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'HISTORY','H_D_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'HISTORY','H_W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'HISTORY','H_DATE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'HISTORY','H_AMOUNT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'HISTORY','H_DATA',UTF8_TYPE)
        
        
        
            self.sys.alter_column(keyspace,'NEW_ORDER','NO_O_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'NEW_ORDER','NO_D_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'NEW_ORDER','NO_W_ID',UTF8_TYPE)
        
            self.sys.alter_column(keyspace,'ORDERS','O_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDERS','O_D_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDERS','O_W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDERS','O_C_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDERS','O_ENTRY_D',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDERS','O_CARRIER_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDERS','O_OL_CNT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDERS','O_ALL_LOCAL',UTF8_TYPE)
        
        
        
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_O_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_D_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_NUMBER',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_I_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_SUPPLY_W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_DELIVERY_D',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_QUANTITY',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_AMOUNT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ORDER_LINE','OL_DIST_INFO',UTF8_TYPE)
        
        
            self.sys.alter_column(keyspace,'ITEM','I_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ITEM','I_IM_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ITEM','I_NAME',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ITEM','I_PRICE',UTF8_TYPE)
            self.sys.alter_column(keyspace,'ITEM','I_DATA',UTF8_TYPE)
        
        
            self.sys.alter_column(keyspace,'STOCK','S_I_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_W_ID',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_QUANTITY',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_01',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_02',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_03',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_04',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_05',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_06',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_07',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_08',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_09',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DIST_10',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_YTD',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_ORDER_CNT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_REMOTE_CNT',UTF8_TYPE)
            self.sys.alter_column(keyspace,'STOCK','S_DATA',UTF8_TYPE)
        
        
            self.sys.create_index(keyspace,'CUSTOMER','C_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'CUSTOMER','C_D_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'CUSTOMER','C_W_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'CUSTOMER','C_LAST', UTF8_TYPE)
            self.sys.create_index(keyspace,'NEW_ORDER','NO_O_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'NEW_ORDER','NO_D_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'NEW_ORDER','NO_W_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'ORDERS','O_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'ORDERS','O_D_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'ORDERS','O_W_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'ORDERS','O_C_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'ORDER_LINE','OL_O_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'ORDER_LINE','OL_D_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'ORDER_LINE','OL_W_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'STOCK','S_W_ID', UTF8_TYPE)
            self.sys.create_index(keyspace,'STOCK','S_QUANTITY', UTF8_TYPE)

                
        self.conn = pycassa.connect(str(config["keyspace"]),[connection])
        self.new_ordercf=pycassa.ColumnFamily(self.conn,'NEW_ORDER')
        self.orderscf=pycassa.ColumnFamily(self.conn, 'ORDERS')
        self.order_linecf=pycassa.ColumnFamily(self.conn, 'ORDER_LINE')
        self.customercf=pycassa.ColumnFamily(self.conn, 'CUSTOMER')
        self.warehousecf = pycassa.ColumnFamily(self.conn,'WAREHOUSE')
        self.districtcf = pycassa.ColumnFamily(self.conn, 'DISTRICT')
        self.historycf = pycassa.ColumnFamily(self.conn,'HISTORY')
        self.stockcf = pycassa.ColumnFamily(self.conn,'STOCK')
        self.itemcf = pycassa.ColumnFamily(self.conn, 'ITEM')

    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0:
             return
        logging.debug("loading")
        col_fam = pycassa.ColumnFamily(self.conn, tableName)
        if tableName == 'ITEM':
            for row in tuples:
                row_key = str(row[0]).zfill(5)
                i_id = str(row[0])
                i_im_id = str(row[1])
                i_name = str(row[2])
                i_price = str(row[3])
                i_data = str(row[4])
                col_fam.insert(row_key, {'I_ID':i_id, 'I_IM_ID':i_im_id, 'I_NAME':i_name, 'I_PRICE':i_price, 'I_DATA':i_data})
        if tableName == 'WAREHOUSE':
             if len(tuples[0])!=9: return
             for row in tuples: 
                 row_key = str(row[0]).zfill(5)  #w_ID 
                 w_id = str(row[0])
                 w_name =str(row[1])
                 w_street_1 = str(row[2])
                 w_street_2 = str(row[3])
                 w_city = str(row[4])
                 w_state = str(row[5])
                 w_zip = str(row[6])
                 w_tax = str(row[7])
                 w_ytd = str(row[8])
                 col_fam.insert(row_key,{'W_ID':w_id, 'W_NAME':w_name, 'W_STREET_1': w_street_1, 'W_STREET_2': w_street_2, 'W_CITY':w_city, 'W_STATE':w_state, 'W_ZIP':w_zip, 'W_TAX':w_tax, 'W_YTD':w_ytd})
        if tableName == 'CUSTOMER':
            for row in tuples:
                row_key = str(row[0]).zfill(5)+ str(row[1]).zfill(5)+ str(row[2]).zfill(5)
                c_id = str(row[0])
                c_d_id =str(row[1])
                c_w_id =str(row[2])
                c_first =str(row[3])
                c_middle = str(row[4])
                c_last = str(row[5])
                c_street_1 = str(row[6])
                c_street_2 = str(row[7])
                c_city = str(row[8])
                c_state = str(row[9])
                c_zip = str(row[10])
                c_phone = str(row[11])
                c_since = str(row[12])
                c_credit = str(row[13])
                c_credit_lim = str(row[14])
                c_discount = str(row[15])
                c_balance = str(row[16])
                c_ytd_payment = str(row[17])
                c_payment_cnt = str(row[18])
                c_delivery_cnt = str(row[19])
                c_data = str(row[20])
                col_fam.insert(row_key, {'C_ID':c_id, 'C_D_ID':c_d_id, 'C_W_ID':c_w_id, 'C_FIRST':c_first, 'C_MIDDLE':c_middle, 'C_LAST':c_last, 'C_STREET_1':c_street_1,'C_STREET_2':c_street_2, 'C_CITY':c_city, 'C_STATE':c_state, 'C_ZIP':c_zip, 'C_PHONE':c_phone, 'C_SINCE':c_since, 'C_CREDIT':c_credit, 'C_CREDIT_LIM':c_credit_lim, 'C_DISCOUNT':c_discount, 'C_BALANCE':c_balance, 'C_YTD_PAYMENT':c_ytd_payment, 'C_PAYMENT_CNT':c_payment_cnt, 'C_DELIVERY_CNT':c_delivery_cnt, 'C_DATA':c_data})                

        if tableName == 'ORDERS':
            for row in tuples:
                row_key = str(row[0]).zfill(5)+str(row[1]).zfill(5)+ str(row[2]).zfill(5)
                o_id = str(row[0])
                o_d_id = str(row[1])
                o_w_id = str(row[2])
                o_c_id = str(row[3])
                o_entry_d = str(row[4])
                o_carrier_id = str(row[5])
                o_ol_cnt = str(row[6])
                o_all_local = str(row[7])
                col_fam.insert(row_key,{'O_ID':o_id, 'O_D_ID':o_d_id, 'O_W_ID':o_w_id, 'O_C_ID':o_c_id, 'O_ENTRY_D':o_entry_d, 'O_CARRIER_ID':o_carrier_id, 'O_OL_CNT':o_ol_cnt, 'O_ALL_LOCAL':o_all_local})


        if tableName == 'STOCK':
            for row in tuples:
                row_key = str(row[0]).zfill(5)+str(row[1]).zfill(5)
                s_i_id = str(row[0])
                s_w_id = str(row[1])
                s_quantity = str(row[2])
                s_dist_01 = str(row[3])
                s_dist_02 = str(row[4])
                s_dist_03 = str(row[5])
                s_dist_04 = str(row[6])
                s_dist_05 = str(row[7])
                s_dist_06 = str(row[8])
                s_dist_07 = str(row[9])
                s_dist_08 = str(row[10])
                s_dist_09 = str(row[11])
                s_dist_10 = str(row[12])
                s_ytd = str(row[13])
                s_order_cnt = str(row[14])
                s_remote_cnt = str(row[15])
                s_data = str(row[16])
                col_fam.insert(row_key,{'S_I_ID':s_i_id, 'S_W_ID':s_w_id, 'S_QUANTITY':s_quantity, 'S_DIST_01':s_dist_01,'S_DIST_02':s_dist_02,'S_DIST_03':s_dist_03,'S_DIST_04':s_dist_04,'S_DIST_05':s_dist_05,'S_DIST_06':s_dist_06,'S_DIST_07':s_dist_07,'S_DIST_08':s_dist_08,'S_DIST_09':s_dist_09,'S_DIST_10':s_dist_10, 'S_YTD': s_ytd, 'S_ORDER_CNT':s_order_cnt, 'S_REMOTE_CNT':s_remote_cnt, 'S_DATA':s_data})
        
        if tableName == 'DISTRICT':
            for row in tuples:
                row_key = str(row[0]).zfill(5)+str(row[1]).zfill(5)
                d_id = str(row[0])
                d_w_id = str(row[1])
                d_name = str(row[2])
                d_street_1 = str(row[3])
                d_street_2 = str(row[4])
                d_city = str(row[5])
                d_state = str(row[6])
                d_zip = str(row[7])
                d_tax =str(row[8])
                d_ytd = str(row[9])
                d_next_o_id = str(row[10])
                col_fam.insert(row_key,{'D_ID':d_id, 'D_W_ID':d_w_id, 'D_NAME':d_name, 'D_STREET_1':d_street_1, 'D_STREET_2':d_street_2,'D_CITY':d_city, 'D_STATE':d_state, 'D_ZIP':d_zip, 'D_TAX':d_tax, 'D_YTD':d_ytd, 'D_NEXT_O_ID':d_next_o_id})
                
        if tableName == 'NEW_ORDER':
            for row in tuples:
                row_key = str(row[0]).zfill(5)+str(row[1]).zfill(5)+str(row[2]).zfill(5)
                no_o_id = str(row[0])
                no_d_id = str(row[1])
                no_w_id = str(row[2])
                col_fam.insert(row_key,{'NO_O_ID':no_o_id, 'NO_D_ID':no_d_id, 'NO_W_ID':no_w_id})
        if tableName == 'ORDER_LINE':
            for row in tuples:
                row_key = str(row[0]).zfill(5)+str(row[1]).zfill(5)+str(row[2]).zfill(5)+str(row[3]).zfill(5)
                ol_o_id = str(row[0])
                ol_d_id = str(row[1])
                ol_w_id = str(row[2])
                ol_number = str(row[3])
                ol_i_id = str(row[4])
                ol_supply_w_id = str(row[5])
                ol_delivery_d = str(row[6])
                ol_quantity = str(row[7])
                ol_amount = str(row[8])
                ol_dist_info = str(row[9])
                col_fam.insert(row_key,{'OL_O_ID':ol_o_id, 'OL_D_ID':ol_d_id, 'OL_W_ID':ol_w_id, 'OL_NUMBER':ol_number, 'OL_I_ID':ol_i_id, 'OL_SUPPLY_W_ID':ol_supply_w_id, 'OL_DELIVERY_D': ol_delivery_d, 'OL_QUANTITY':ol_quantity,'OL_AMOUNT':ol_amount, 'OL_DIST_INFO':ol_dist_info})
        
        if tableName == 'HISTORY':
            for i in range(len(tuples)):
                #row_key = str(i)
                row_key = str(uuid.uuid1())
                h_c_id = str(tuples[i][0])
                h_c_d_id = str(tuples[i][1])
                h_c_w_id = str(tuples[i][2])
                h_d_id = str(tuples[i][3])
                h_w_id = str(tuples[i][4])
                h_date = str(tuples[i][5])
                h_amount = str(tuples[i][6])
                h_data = str(tuples[i][7])
                col_fam.insert(row_key, {'H_C_ID':h_c_id, 'H_C_D_ID':h_c_d_id, 'H_C_W_ID':h_c_w_id, 'H_D_ID':h_d_id, 'H_W_ID':h_w_id, 'H_DATE':h_date,'H_AMOUNT':h_amount, 'H_DATA':h_data})
#   print tableName+'--' + str(len(tuples))
                 
    def loadFinish(self):
         logging.info("Commiting changes to database")

            


    ##-----------------------------------
    ## doDelivery
    ##----------------------------------
    def doDelivery(self, params):
        logging.debug("do delivery")    

        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]
        
        
        
        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            did_expr = create_index_expression('NO_D_ID',str(d_id))
            wid_expr = create_index_expression('NO_W_ID',str(w_id))    
            clause = create_index_clause([did_expr,wid_expr],count=1)
            newOrder=self.new_ordercf.get_indexed_slices(clause)
            flag=0
            for key, column in newOrder:
                #print column
                no_o_id=column['NO_O_ID']
                flag=1
            if flag==0:
                continue
            if int(no_o_id)<=-1:
                continue
            if no_o_id==None:
                continue
    #        print no_o_id
    #        print d_id
    #        print w_id
            orders_rowkey=no_o_id.zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5)
            #print orders_rowkey
            o=self.orderscf.get(orders_rowkey)
            

            c_id=str(o['O_C_ID'])
        
            oid_expr = create_index_expression('OL_O_ID',str(no_o_id))
            did_expr = create_index_expression('OL_D_ID',str(d_id))
            wid_expr = create_index_expression('OL_W_ID',str(w_id))

            clause = create_index_clause([oid_expr,did_expr,wid_expr],count=100000)
            orderLine=self.order_linecf.get_indexed_slices(clause)

            ol_total=0
            for key, column in orderLine:
                ol_total+=float(column['OL_AMOUNT'])

            deleteKey=no_o_id.zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5)
            self.new_ordercf.remove(deleteKey)
            self.orderscf.insert(deleteKey, {'O_CARRIER_ID': str(o_carrier_id)})
            self.order_linecf.insert(deleteKey,{'OL_DELIVERY_D':str(ol_delivery_d)})

            c=self.customercf.get(str(c_id).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5))
            old_balance=float(c['C_BALANCE'])
            new_balance=str(old_balance+ol_total)
            self.customercf.insert(str(c_id).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5),{'C_BALANCE': str(new_balance)})
                
            result.append((str(d_id),str(no_o_id)))
        ##for
        
        return result
    ##-----------------------------------
    ## doNewOrder
    ##-----------------------------------

    def doNewOrder(self, params):
        logging.debug("do new order")
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]        


        assert len(i_ids) > 0
        assert len(i_ids) ==len(i_w_ids)
        assert len(i_ids) ==len(i_qtys)

        all_local = True
        items = [ ]
        for i in range(len(i_ids)):
            all_local = all_local and i_w_ids[i] == w_id
            ol_i_id = i_ids[i]
            itm=self.itemcf.get(str(ol_i_id).zfill(5), columns=['I_PRICE','I_NAME','I_DATA'])
            items.append(itm)   
        assert len(items)==len(i_ids)
            

        for itm in items:
            if len(itm)==0:
                return

        

        
            
        #getWarehouseTaxRate
        w_tax_c = self.warehousecf.get(str(w_id).zfill(5),columns=['W_TAX'])
        w_tax =float(w_tax_c['W_TAX'])
        #getDistrict
        row_key = str(d_id).zfill(5) +str(w_id).zfill(5) 
        o=self.districtcf.get(row_key, columns=['D_TAX','D_NEXT_O_ID'])
        d_tax = float(o['D_TAX'])
        #incrementNextOrderId
        d_next_o_id = int(o['D_NEXT_O_ID'])
            
        #getCustomer
        row_key = str(c_id).zfill(5) +str(d_id).zfill(5)+str(w_id).zfill(5)
        customer_info = self.customercf.get(row_key,columns=['C_DISCOUNT','C_LAST','C_CREDIT'])
        c_discount=float(customer_info['C_DISCOUNT'])
        
        o_carrier_id = constants.NULL_CARRIER_ID
        ol_cnt = len(i_ids)
    
        #incrementNextOrderId
        row_key = str(d_id).zfill(5)+str(w_id).zfill(5)
        self.districtcf.insert(row_key,{'D_NEXT_O_ID':str(d_next_o_id+1)})
            
        #createOrder
        
        order_rowkey=str(d_next_o_id).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5)
    #    print "d_next_o_id " +str(d_next_o_id) 
    #    print "d_id "+str(d_id)
    #    print "order_rowkey " + order_rowkey
        self.orderscf.insert(order_rowkey,{'O_ID':str(d_next_o_id), 'O_D_ID':str(d_id), 'O_W_ID':str(w_id), 'O_C_ID':str(c_id), 'O_ENTRY_D':str(o_entry_d), 'O_CARRIER_ID':str(o_carrier_id), 'O_OL_CNT':str(ol_cnt), 'O_ALL_LOCAL':str(all_local)})
            
        #createNewOrder
        neworder_rowkey=str(d_next_o_id).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5)
    #    print 'neworder_rowkey ' + neworder_rowkey
        self.new_ordercf.insert(neworder_rowkey, {'NO_O_ID':str(d_next_o_id), 'NO_D_ID':str(d_id), 'NO_W_ID':str(w_id)})
        #getItemInfo
        total = 0
        item_data = [ ]
        for i in range(len(i_ids)):
            itemInfo = items[i]
            i_name = itemInfo['I_NAME']
            i_data = itemInfo['I_DATA']
            i_price =float(itemInfo['I_PRICE'])

        #"getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
            ol_i_id = i_ids[i]
            ol_number  = i+1
            ol_supply_w_id = i_w_ids[i]
            ol_quantity = i_qtys[i]
    
            stockInfo = self.stockcf.get(str(ol_i_id).zfill(5)+str(ol_supply_w_id).zfill(5))
        #"updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
            if len(stockInfo)==0:
                 logging.warn("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
                 continue
            s_quantity = int(stockInfo['S_QUANTITY'])
            s_ytd = int(stockInfo['S_YTD'])
            s_order_cnt = int(stockInfo['S_ORDER_CNT'])
            s_remote_cnt = int(stockInfo['S_REMOTE_CNT'])
            s_data = stockInfo['S_DATA']
            if d_id < 10:
                s_dist_col='S_DIST_'+'0'+str(d_id)
            else:
                s_dist_col='S_DIST_'+str(d_id)
            s_dist_xx = stockInfo[s_dist_col]
            
                
            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            if ol_supply_w_id != w_id: s_remote_cnt += 1
            self.stockcf.insert(str(ol_i_id).zfill(5)+str(ol_supply_w_id).zfill(5),{'S_QUANTITY':str(s_quantity), 'S_YTD':str(s_ytd), 'S_ORDER_CNT':str(s_order_cnt) , 'S_REMOTE_CNT':str(s_remote_cnt)})

            ##"createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING)!= -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            ol_amount = ol_quantity * i_price
            total += ol_amount

            orderline_rowkey=str(d_next_o_id).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5)
            self.order_linecf.insert(orderline_rowkey,{'OL_O_ID': str(d_next_o_id), 'OL_D_ID':str(d_id), 'OL_W_ID':str(w_id), 'OL_NUMBER':str(ol_number), 'OL_I_ID':str(ol_i_id), 'OL_SUPPLY_W_ID':str(ol_supply_w_id), 'OL_DELIVERY_D': str(o_entry_d), 'OL_QUANTITY':str(ol_quantity),'OL_AMOUNT':str(ol_amount), 'OL_DIST_INFO':str(s_dist_xx)})
            item_data.append( (i_name, s_quantity, brand_generic,i_price, ol_amount) )
        total *= (1 - c_discount) * (1 + w_tax + d_tax)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        return [ customer_info, misc, item_data ]
        ##----------------------------
    ## doPayment
    ##----------------------------

    def doPayment(self, params):
        logging.debug("do payment")
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        
        if c_id != None:
            #getCustomerByCustomerId
            row_key = str(c_id).zfill(5) +str(d_id).zfill(5)+str(w_id).zfill(5)
            customer = self.customercf.get(row_key)
            assert len(customer)>0
            c_balance = float(str(customer['C_BALANCE']))- h_amount
            c_ytd_payment = float(str(customer['C_YTD_PAYMENT'])) + h_amount
            c_payment_cnt = int(str(customer['C_PAYMENT_CNT']))+1
            c_data = str(customer['C_DATA'])
            c_credit = str(customer['C_CREDIT'])
        else:
            #getCustomerByLastName
            c_1_expr = create_index_expression('C_W_ID',str(w_id))
            c_2_expr = create_index_expression('C_D_ID',str(d_id))
            c_3_expr = create_index_expression('C_LAST',str(c_last))
            clause = create_index_clause([c_1_expr,c_2_expr,c_3_expr],count=1000)
            
            newcustomer = self.customercf.get_indexed_slices(clause)
            firstnames=[]
            
            namecnt=0
            for key, column in newcustomer:
                firstnames.append(column['C_FIRST'])
                namecnt+=1
        #    print namecnt
            index = (namecnt-1)/2
            firstname=firstnames[index]
            c_4_expr = create_index_expression('C_LAST',str(c_last))
            clause = create_index_clause([c_1_expr,c_2_expr,c_3_expr,c_4_expr],count=1)
            newcustomer = self.customercf.get_indexed_slices(clause)
            for key, column in newcustomer:
                c_id = column['C_ID']
                c_balance = float(column['C_BALANCE'])- h_amount
                c_ytd_payment = float(column['C_YTD_PAYMENT']) + h_amount
                c_payment_cnt = int(column['C_PAYMENT_CNT'])+1
                c_data = column['C_DATA']
                c_credit =column['C_CREDIT']
            row_key = str(c_id).zfill(5) +str(d_id).zfill(5)+str(w_id).zfill(5)
            customer = self.customercf.get(row_key)
        warehouse = self.warehousecf.get(str(w_id).zfill(5))
        district = self.districtcf.get(str(d_id).zfill(5)+str(w_id).zfill(5))

        self.warehousecf.insert(str(w_id).zfill(5),{'W_YTD':str(float(warehouse['W_YTD'])+h_amount)})
        
        self.districtcf.insert(str(d_id).zfill(5)+str(w_id).zfill(5),{'D_YTD': str(float(district['D_YTD'])+h_amount)})
        
        if c_credit == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            self.customercf.insert(str(c_id).zfill(5)+str(c_d_id).zfill(5)+str(c_w_id).zfill(5),{ 'C_BALANCE' : str(c_balance), 'C_YTD_PAYMENT':str(c_ytd_payment) , 'C_PAYMENT_CNT':str(c_payment_cnt), 'C_DATA' : str(c_data)})
        else:
            c_data = ""
            self.customercf.insert(str(c_id).zfill(5)+str(c_d_id).zfill(5)+str(c_w_id).zfill(5),{ 'C_BALANCE' : str(c_balance), 'C_YTD_PAYMENT':str(c_ytd_payment) , 'C_PAYMENT_CNT':str(c_payment_cnt)})        
        h_data= "%s    %s" % (warehouse['W_NAME'], district['D_NAME'])
        self.historycf.insert(str(uuid.uuid1()), {'H_C_ID':str(c_id), 'H_C_D_ID':str(c_d_id), 'H_C_W_ID':str(c_w_id), 'H_D_ID':str(d_id), 'H_W_ID':str(w_id), 'H_DATE':str(h_date),'H_AMOUNT':str(h_amount), 'H_DATA':str(h_data)})
        return [warehouse, district, customer]
    ##-----------------------------------
    ## doOrderStatus
    ##-----------------------------------
    def doOrderStatus(self, params):
        logging.info("do orderStatus")
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]

        assert w_id, pformat(params)
        assert d_id, pformat(params)
        

        if c_id == None:
            last_expr = create_index_expression('C_LAST',str(c_last))
            did_expr = create_index_expression('C_D_ID',str(d_id))
            wid_expr = create_index_expression('C_W_ID',str(w_id))
            clause = create_index_clause([last_expr,did_expr,wid_expr],count=10000)
            all_customers=self.customercf.get_indexed_slices(clause)
            first_names=[ ]
            c_ids=[]
            namecnt=0
            for key, column in all_customers:
                first_names.append(column['C_FIRST'])
                c_ids.append(column['C_ID'])
                namecnt = namecnt+1
            namecnt=len(first_names)
            assert namecnt>0
            index=(namecnt-1)/2
            first_name=first_names[index]
            assert first_name!=None
            c_id=c_ids[index]
            assert c_id!=None

        key1=str(c_id).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5)
        res1=self.customercf.get(key1)
        customer=[res1['C_ID'],res1['C_FIRST'],res1['C_MIDDLE'],res1['C_LAST'],res1['C_BALANCE']]

        cid_expr = create_index_expression('O_C_ID',str(c_id))
        did_expr = create_index_expression('O_D_ID',str(d_id))
        wid_expr = create_index_expression('O_W_ID',str(w_id))
        clause = create_index_clause([cid_expr,did_expr,wid_expr],count=100000)
        all_orders=self.orderscf.get_indexed_slices(clause)
        
        last_order_oid=0
        order=[]
        for key, column in all_orders:
            if int(column['O_ID'])>last_order_oid:
                last_order_oid=int(column['O_ID'])
        if last_order_oid>0:
            o=self.orderscf.get(str(last_order_oid).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5))
            order=[o['O_ID'],o['O_CARRIER_ID'],o['O_ENTRY_D']]
        
        
        orderLines = []
        if last_order_oid>0:
            oid_expr = create_index_expression('OL_O_ID',str(last_order_oid))
            did_expr = create_index_expression('OL_D_ID',str(d_id))
            wid_expr = create_index_expression('OL_W_ID',str(w_id))
            clause = create_index_clause([oid_expr,did_expr,wid_expr])
            orderLine=self.order_linecf.get_indexed_slices(clause)
            for key, column in orderLine:
                orderLines.append([column['OL_SUPPLY_W_ID'],column['OL_I_ID'],column['OL_QUANTITY'],column['OL_AMOUNT'],column['OL_DELIVERY_D']])

        return [ customer, order, orderLines ]

        ##----------------------------
    ## doStockLevel
    ##----------------------------


    def doStockLevel(self, params):
        logging.info("do stocklevel")
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

    
        #"getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", 
        d = self.districtcf.get(str(d_id).zfill(5)+str(w_id).zfill(5),columns=['D_NEXT_O_ID'])
        assert d
        #getStockCount
        o_id = d['D_NEXT_O_ID']
    
        
        s_q_expr = create_index_expression('S_QUANTITY',str(threshold), LT)
        s_q_expr2 = create_index_expression('S_W_ID',str(w_id))
        clause = create_index_clause([s_q_expr,s_q_expr2])
        newstock = self.stockcf.get_indexed_slices(clause)


        ol_expr = create_index_expression('OL_W_ID',str(w_id))
        ol_expr2 = create_index_expression('OL_D_ID',str(d_id))
        ol_expr3 = create_index_expression('OL_O_ID',str(o_id),LT)
        ol_expr4 = create_index_expression('OL_O_ID', str(int(o_id)-20),GTE)
        clause2 = create_index_clause([ol_expr,ol_expr2])
        neworderline = self.order_linecf.get_indexed_slices(clause2)
        
        count = 0
        for key, column in newstock:
            for key2, column2 in neworderline:
                tmp1 =  column['S_I_ID']
                s_i_id = int(tmp1)
                tmp2 = column2['OL_I_ID']
                ol_i_id = int(tmp2)
                if s_i_id == ol_i_id:
                    count= count+1
        
        return count                

