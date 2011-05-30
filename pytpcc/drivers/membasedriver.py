# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Sunil Mallya, Silvia Zuffi
# http://www.cs.brown.edu/~sunilmallya/
# http://www.cs.brown.edu/~zuffi/
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

import os,time
import logging
import commands
import memcache
from pprint import pprint,pformat

import constants
from abstractdriver import *
MAX_CUSTOMER_ID = 3000
MAX_ORDER_ID = 2999

# Appended with number of columns that needs to be used as the key
TABLE_COLUMNS = {
    constants.TABLENAME_ITEM: [
        "I_ID", # INTEGER
        "I_IM_ID", # INTEGER
        "I_NAME", # VARCHAR
        "I_PRICE", # FLOAT
        "I_DATA", # VARCHAR
        1
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
        1
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
        2
    ],
    constants.TABLENAME_CUSTOMER: [
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
        3
    ],
    constants.TABLENAME_STOCK: [
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
        2
    ],
    constants.TABLENAME_ORDERS: [
        "O_ID", # INTEGER
        "O_C_ID", # INTEGER
        "O_D_ID", # TINYINT
        "O_W_ID", # SMALLINT
        "O_ENTRY_D", # TIMESTAMP
        "O_CARRIER_ID", # INTEGER
        "O_OL_CNT", # INTEGER
        "O_ALL_LOCAL", # INTEGER
        4
    ],
    "ORDERS_ID": [
        "O_C_ID", # INTEGER
        "O_D_ID", # TINYINT
        "O_W_ID", # SMALLINT
        "O_ID", # VARCHAR
        3
    ],
    constants.TABLENAME_NEW_ORDER: [
        "NO_O_ID", # INTEGER
        "NO_D_ID", # TINYINT
        "NO_W_ID", # SMALLINT
        3
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
        3
    ],
    constants.TABLENAME_HISTORY: [
        "H_C_ID", # INTEGER
        "H_C_D_ID", # TINYINT
        "H_C_W_ID", # SMALLINT
        "H_D_ID", # TINYINT
        "H_W_ID", # SMALLINT
        "H_DATE", # TIMESTAMP
        "H_AMOUNT", # FLOAT
        "H_DATA", # VARCHAR
        5
    ],
}

def irange(sequence):
    return zip(range(len(sequence)),sequence)

def filter_table(sec_keys,table_values,table_name):
    
    columns = TABLE_COLUMNS[table_name] 
    return_list=[]
    sec_key_indices = [] # [index, value]
    for sec_key_name in sec_keys:
        if sec_key_name[0]!= None:
            index = columns.index(sec_key_name[0])
            sec_key_indices.append([index,sec_key_name[1]])

    for tvalue in table_values:
        copy_flag = 1
        for sec_key_tuple in sec_key_indices:
            if tvalue[sec_key_tuple[0]] != sec_key_tuple[1]:
                copy_flag =0
            
            if copy_flag ==1:
                return_list.append(tvalue)
                
    return  return_list


def return_columns_single_record(extract_columns,table_values,table_name):
    columns = TABLE_COLUMNS[table_name] 
    record =[]
    for c in extract_columns:
        c_index = columns.index(c)
        record.append(table_values[c_index])
 
    return record

def return_columns(extract_columns,table_values,table_name):
    #c contains the name of the columns to be extracted
    columns = TABLE_COLUMNS[table_name] 

    return_table=[]
    for tvalue in table_values:
        insert_tuple=[]
        for c in extract_columns:
            c_index = columns.index(c)
            insert_tuple.append(tvalue[c_index])
        return_table.append(insert_tuple)
        
    return return_table


## ==============================================
## MembaseDriver
## ==============================================
class MembaseDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "host": ("The hostname to membase", "localhost" ),
        "port": ("The port number to membase", 11211 ),
        "name": ("Collection name", "tpcc"),
    }
    #This may be similar to the memcache configuration
    
    def __init__(self, ddl):
        super(MembaseDriver, self).__init__("membase", ddl)
        self.database = None
        self.conn = None
        self.load_time = 0      
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return MembaseDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in MembaseDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        connection_string = config['host'] +":"+ str(config['port'])
        conn_list = [] ; conn_list.append(connection_string)
        
        self.conn = memcache.Client(conn_list)
        #self.database = self.conn[str(config['name'])]
        
    ## ----------------------------------------------
    ## loadTuples into a csv file
    ## ../py-tpcc/src/pytpcc/insert_data.csv
    ## ----------------------------------------------

    def loadTuples_to_csv(self, tableName, tuples):
        
        if len(tuples) == 0: return
        f = open("insert_data.csv",'a')
        for tuple_element in tuples:
            primary_key = tuple_element[0]
            key = tableName +"_"+ str(primary_key)
            value = tuple_element[1:]
            value = str(value)
            value = value.strip('['); value = value.strip(']');
            f.write(key+','+value+"\n")
        f.close()

    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: 
            print "NO DATA"
            return
        temp_max_id = self.conn.get(tableName+"_max_pkey")
        if temp_max_id == None:
            self.conn.set(tableName+"_max_pkey",0)     
            temp_max_id = 0
  
        columns = TABLE_COLUMNS[tableName] 
        key_ids = columns[-1]
    
        key_val_pairs={}
        start_time = time.time()
        for tuple_element in tuples:
            primary_key = tuple_element[0]
            if temp_max_id < primary_key:
                temp_max_id = primary_key

            key_postfix =""
            for idx in range(0,key_ids): 
                key_postfix = key_postfix + "_" + str(tuple_element[idx])
                
            key = tableName + key_postfix
            value = tuple_element
            key_val_pairs[key] = value
            #tmp_dict={}
            #tmp_dict[key]=value
            #key_val_pairs.append(tmp_dict)
            
            #self.conn.set(key,value)
            
            """
            if tableName == "ORDERS":
                okey = "ORDERS_ID_"+str(tuple_element[1])+"_"+str(tuple_element[2])+"_"+str(tuple_element[3])
                ovalue = str(tuple_element[0])+":"
                ret = self.conn.append(okey, ovalue)
                if ret == False:
                self.conn.set(okey, ovalue)
            """
            if tableName == "ORDERS":
                okey = "ORDERS_ID_"+str(tuple_element[1])+"_"+str(tuple_element[2])+"_"+str(tuple_element[3])
                ovalue = self.conn.get(okey)
                if ovalue == None:
                    ovalue = str(tuple_element[0])+":"
                else:
                    ovalue = ovalue + str(tuple_element[0])+":"
                key_val_pairs[okey] = ovalue
            
        #Use multi_set to send all the keys to vbucket
        self.conn.set_multi(key_val_pairs)
        
        #Define the implicit Schema
        end_time = time.time()
        self.load_time += (end_time - start_time)
        self.conn.replace(tableName+"_max_pkey",temp_max_id)     
        print "[",tableName,"]"," Current Load Time: ",self.load_time
        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        return

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------

    def loadFinish(self):
        logging.info("Commiting changes to database")
    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------


    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):

        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["o_carrier_id"]
        
        result = [ ]
        ol_total =0
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
        
            #-------------------------------------------------------------------------------------
            # The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID) 
            # and with the lowest NO_O_ID value is selected.
            #-------------------------------------------------------------------------------------
            
            for idx in range(1, MAX_ORDER_ID+1):
                new_order = self.conn.get("NEW_ORDER_"+str(idx)+"_"+str(d_id)+"_"+str(w_id))
                if new_order != None:
                    break

            if new_order == None:
                continue
            
            assert len(new_order) > 0
            columns = TABLE_COLUMNS["NEW_ORDER"] 
            o_id = new_order[ columns.index("NO_O_ID") ] 
            
            #-------------------------------------------------------------------------------------
            # The selected row in the NEW-ORDER table is deleted.
            #-------------------------------------------------------------------------------------
            
            self.conn.delete("NEW_ORDER_"+str(o_id)+"_"+str(d_id)+"_"+str(w_id))
         
            
            #-------------------------------------------------------------------------------------
            # The row in the ORDER table with matching O_W_ID (equals W_ ID), O_D_ID (equals D_ID), 
            # and O_ID (equals NO_O_ID) is selected, O_C_ID, the customer number, is retrieved, 
            # and O_CARRIER_ID is updated.
            #-------------------------------------------------------------------------------------
            
            # getCId
            for idx in range(1, MAX_CUSTOMER_ID+1):
                order = self.conn.get("ORDERS_"+str(o_id)+"_"+str(idx)+"_"+str(d_id)+"_"+str(w_id))
                if order != None:
                    c_id = idx;
                    break



            # "updateOrders"
            updated_order = self.update_single_record("O_CARRIER_ID", o_carrier_id, order,"ORDERS");
            self.conn.set("ORDERS_"+str(o_id)+"_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id),updated_order);

            #-------------------------------------------------------------------------------------
            # All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), 
            # and OL_O_ID (equals O_ID) are selected. All OL_DELIVERY_D, the delivery dates, are updated to 
            # the current system time as returned by the operating system and the sum of all OL_AMOUNT is retrieved.
            #-------------------------------------------------------------------------------------

            order_line = self.conn.get("ORDER_LINE_"+str(o_id)+"_"+str(d_id)+"_"+str(w_id))
            updated_order_line = self.update_single_record("OL_DELIVERY_D", ol_delivery_d, order_line,"ORDER_LINE")
            self.conn.set("ORDER_LINE_"+str(o_id)+"_"+str(d_id)+"_"+str(w_id), updated_order_line)

            columns = TABLE_COLUMNS["ORDER_LINE"] 
            selected_order_line = order_line[ columns.index("OL_AMOUNT") ]
            
            ol_total += selected_order_line

            #-------------------------------------------------------------------------------------
            # The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID (equals D_ID), 
            # and C_ID (equals O_C_ID) is selected and C_BALANCE is increased by the sum of all order-line 
            # amounts (OL_AMOUNT) previously retrieved. C_DELIVERY_CNT is incremented by 1.
            #-------------------------------------------------------------------------------------

            # "updateCustomer": 
            customer = self.conn.get("CUSTOMER_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id))
            updated_customer = self.increment_field("C_BALANCE", ol_total, customer,"CUSTOMER")
            updated_customer = self.increment_field("C_DELIVERY_CNT", 1, updated_customer,"CUSTOMER")
            self.conn.set("CUSTOMER_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id), updated_customer);
            
            result.append((d_id, o_id))
        return result


    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        def __getItem(items,item_id):
            for index,item in irange(items):
                if item[0] == item_id:
                    return index 
            
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]

        o_entry_d = params["o_entry_d"]
        s_dist_col = "S_DIST_%02d" % d_id
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)
        
        items = []
        for item_index in i_ids:
            item_key = "ITEM_"+str(item_index)
            new_item = self.conn.get(item_key)
            if new_item != None:
                items.append(new_item)

        #-------------------------------------------------------------------------------------
        # For each item on the order:
        # The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected and I_PRICE, 
        # the price of the item, I_NAME, the name of the item, and I_DATA are retrieved. 
        # If I_ID has an unused value, a "not-found" condition is signaled, 
        # resulting in a rollback of the database transaction.
        #-------------------------------------------------------------------------------------

        if len(items) != len(i_ids):
            return
            
        total = 1
        item_data = []
        
        #-------------------------------------------------------------------------------------

        # The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse tax rate, is retrieved.
        #-------------------------------------------------------------------------------------
        warehouse = self.conn.get("WAREHOUSE_"+str(w_id))
        assert warehouse
        columns = TABLE_COLUMNS["WAREHOUSE"] 
        w_tax = warehouse[ columns.index("W_TAX") ]

        #-------------------------------------------------------------------------------------
        # The row in the DISTRICT table with matching D_W_ID and D_ ID is selected, D_TAX, the district tax rate, 
        # is retrieved, and D_NEXT_O_ID, the next available order number for the district, 
        # is retrieved and incremented by one.
        #-------------------------------------------------------------------------------------
        district = self.conn.get("DISTRICT_"+str(d_id)+"_"+str(w_id))
        assert district
        selected_district = return_columns_single_record(["D_TAX", "D_NEXT_O_ID"], district, "DISTRICT")
        d_tax = selected_district[0]
        d_next_o_id = selected_district[1]
        
        updated_district = self.increment_field("D_NEXT_O_ID", 1, district, "DISTRICT");
        self.conn.set("DISTRICT_"+str(d_id)+"_"+str(w_id), updated_district);
        
        #-------------------------------------------------------------------------------------
        # The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected and C_DISCOUNT, 
        # the customer's discount rate, C_LAST, the customer's last name, and C_CREDIT, the customer's credit status, 
        # are retrieved.
        #-------------------------------------------------------------------------------------
        customer = self.conn.get("CUSTOMER_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id))
        assert customer
        selected_customer = return_columns_single_record(["C_DISCOUNT", "C_LAST", "C_CREDIT"], customer, "CUSTOMER")
        c_discount = selected_customer[0]
        
        #-------------------------------------------------------------------------------------
        # A new row is inserted into both the NEW-ORDER table and the ORDER table to reflect the creation
        # of the new order. O_CARRIER_ID is set to a null value. If the order includes only home order-lines, 
        # then O_ALL_LOCAL is set to 1, otherwise O_ALL_LOCAL is set to 0.
        # The number of items, O_OL_CNT, is computed to match ol_cnt. 
        #-------------------------------------------------------------------------------------
        # from mongodb driver
        all_local = (not i_w_ids or [w_id] * len(i_w_ids) == i_w_ids)
       
        o_all_local = 0 
        if all_local:
            o_all_local = 1;
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        value = [o_entry_d, o_carrier_id, ol_cnt, all_local]
        self.conn.set("ORDERS_"+str(d_next_o_id)+"_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id), value)

        # update the table that stores the orders id
        ret = self.conn.append("ORDERS_ID_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id), str(d_next_o_id)+":")
        if ret == None:
            self.conn.set("ORDERS_ID_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id), str(d_next_o_id)+":")

        self.conn.set("NEW_ORDERS_"+str(d_next_o_id)+"_"+str(d_id)+"_"+str(w_id),value)

        for idx in i_ids:

            #-------------------------------------------------------------------------------------
            # For each O_OL_CNT item on the order:
            # The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected and I_PRICE, 
            # the price of the item, I_NAME, the name of the item, and I_DATA are retrieved. 
            # If I_ID has an unused value, a "not-found" condition is signaled, 
            # resulting in a rollback of the database transaction.
            #-------------------------------------------------------------------------------------
            
            i = __getItem(items,idx)
            item = self.conn.get("ITEM_"+str(idx));
            if item == None:
                return

            selected_item = return_columns_single_record(["I_PRICE", "I_NAME", "I_DATA"], item, "ITEM");
            i_price = selected_item[0]
            i_name = selected_item[1]
            i_data = selected_item[2]

            
            #-------------------------------------------------------------------------------------
            # The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals OL_SUPPLY_W_ID) 
            # is selected. S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the district number, 
            # and S_DATA are retrieved. If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or more, 
            # then S_QUANTITY is decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91.
            # S_YTD is increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1. If the order-line is remote, then
            # S_REMOTE_CNT is incremented by 1.
            #-------------------------------------------------------------------------------------

            ol_supply_w_id = i_w_ids[i]
            stock = self.conn.get("STOCK_"+str(idx)+"_"+str(ol_supply_w_id))
            assert stock 

            selected_stock = return_columns_single_record(["S_QUANTITY", "S_DATA", "S_YTD", "S_ORDER_CNT", "S_REMOTE_CNT", s_dist_col ], stock, "STOCK")
            s_quantity = selected_stock[0]
            s_data = selected_stock[1]
            ol_quantity = i_qtys[i]
            if(s_quantity > (ol_quantity+10)):
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = (s_quantity - ol_quantity)+91
                
            updated_stock = self.update_single_record("S_QUANTITY", s_quantity, stock,"STOCK");
            updated_stock = self.increment_field("S_YTD", ol_quantity, updated_stock, "STOCK")
            updated_stock = self.increment_field("S_ORDER_CNT", 1, updated_stock, "STOCK")
            if o_all_local == 1:
                updated_stock = self.increment_field("S_REMOTE_CNT", 1, updated_stock, "STOCK")
            
            self.conn.set("STOCK_"+str(idx)+"_"+str(ol_supply_w_id), updated_stock);
            
            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_quantity) )
        
        total *= (1 - c_discount) * (1 + w_tax + d_tax)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
 
        return [ customer, misc, item_data ]


    ## ----------------------------------------------
    ## doOrderStatus
    def doOrderStatus(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]

        
        assert w_id, pformat(params)
        assert d_id, pformat(params)
        
        #-------------------------------------------------------------------------------------
        # Case 1, the customer is selected based on customer number: the row in the CUSTOMER table with matching 
        # C_W_ID, C_D_ID, and C_ID is selected and C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved.
        #-------------------------------------------------------------------------------------
        
        if c_id != None:
            customer = self.conn.get("CUSTOMER_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id))
        
        #-------------------------------------------------------------------------------------
        # Case 2, the customer is selected based on customer last name: all rows in the CUSTOMER table with 
        # matching C_W_ID, C_D_ID and C_LAST are selected sorted by C_FIRST in ascending order. 
        # Let n be the number of rows selected. C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved from 
        # the row at position n/2 rounded up in the sorted set of selected rows from the CUSTOMER table.
        #-------------------------------------------------------------------------------------
        else:
            all_customers= []
            for idx in range(1, MAX_CUSTOMER_ID+1):
                customer_record = self.conn.get("CUSTOMER_"+str(idx)+"_"+str(d_id)+"_"+str(w_id))
                all_customers.append(customer_record)
            filtered_customer_table = filter_table([["C_LAST",c_last]],all_customers,"CUSTOMER")
            ordered_customer_map = self.order_by("C_FIRST", filtered_customer_table, "CUSTOMER");
            
            # Get the midpoint customer's id
            namecnt = len(filtered_customer_table)
            assert namecnt > 0
            index = ordered_customer_map[(namecnt-1)/2][0]
            customer = filtered_customer_table[index]

            columns = TABLE_COLUMNS["CUSTOMER"]
            c_index =  columns.index('C_ID')
            c_id = customer[c_index]

        selected_customer = return_columns_single_record(["C_BALANCE", "C_FIRST", "C_MIDDLE", "C_LAST"], customer, "CUSTOMER")

        #-------------------------------------------------------------------------------------
        # The row in the ORDER table with matching O_W_ID (equals C_W_ID), O_D_ID (equals C_D_ID), O_C_ID (equals C_ID), 
        # and with the largest existing O_ID, is selected. This is the most recent order placed by that customer. 
        # O_ID, O_ENTRY_D, and O_CARRIER_ID are retrieved.
        #-------------------------------------------------------------------------------------•

        # read all the orders id from table
        all_orders_id_str = self.conn.get("ORDERS_ID_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id))
        assert all_orders_id_str 
        all_orders_id_array = all_orders_id_str.split(":");
        o_id = 0;

    # the last element is always empty because we append : at the end
        for j in range(len(all_orders_id_array)-1):
            o_str = all_orders_id_array[j]
            o = int(o_str)
            if o > o_id: 
                o_id = o
        order = self.conn.get("ORDERS_"+str(o_id)+"_"+str(c_id)+"_"+str(d_id)+"_"+str(w_id))



        #-------------------------------------------------------------------------------------
        # All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), 
        # and OL_O_ID (equals O_ID) are selected and the corresponding sets of OL_I_ID, OL_SUPPLY_W_ID, 
        # OL_QUANTITY, OL_AMOUNT, and OL_DELIVERY_D are retrieved.
        #-------------------------------------------------------------------------------------

        order_lines = self.conn.get("ORDER_LINE_"+str(o_id)+"_"+str(d_id)+"_"+str(w_id))
        filter_order_lines = return_columns_single_record(["OL_SUPPLY_W_ID", "OL_I_ID", "OL_QUANTITY", "OL_AMOUNT", "OL_DELIVERY_D"],order_lines, "ORDER_LINE")
        return [ selected_customer, order, filter_order_lines ]
        
        
    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
       	#print "PAYMENT" 
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_amount = params["h_amount"];
        h_date = params["h_date"]
        # if w_id = c_w_id the transaction is "home", otherwise "remote"


        #-------------------------------------------------------------------------------------
        # The row in the WAREHOUSE table with matching W_ID is selected. 
        # W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved 
        # and W_YTD, the warehouse's year-to-date balance, is increased by H_ AMOUNT.
        #-------------------------------------------------------------------------------------

        warehouse = self.conn.get("WAREHOUSE_"+str(w_id))
        updated_warehouse = self.increment_field("W_YTD", h_amount, warehouse,"WAREHOUSE")
        self.conn.set("WAREHOUSE_"+str(w_id), updated_warehouse);
        selected_warehouse = return_columns_single_record(["W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP"], updated_warehouse, "WAREHOUSE")

        #-------------------------------------------------------------------------------------
        # The row in the DISTRICT table with matching D_W_ID and D_ID is selected. 
        # D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved and D_YTD, 
        # the district's year-to-date balance, is increased by H_AMOUNT.
        #-------------------------------------------------------------------------------------

        district = self.conn.get("DISTRICT_"+str(d_id)+"_"+str(w_id))
        selected_district = return_columns_single_record(["D_NAME", "D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP"], district, "DISTRICT")
        updated_district = self.increment_field("D_YTD", h_amount, district,"DISTRICT")
        self.conn.set("DISTRICT_"+str(d_id)+"_"+str(w_id),updated_district)
        
        #-------------------------------------------------------------------------------------
        # Case 1, the customer is selected based on customer number: the row in the CUSTOMER table with 
        # matching C_W_ID, C_D_ID and C_ID is selected. C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, 
        # C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, and C_BALANCE are retrieved. 
        # C_BALANCE is decreased by H_AMOUNT. C_YTD_PAYMENT is increased by H_AMOUNT. C_PAYMENT_CNT is incremented by 1.  
        #-------------------------------------------------------------------------------------

        if c_id != None:
            customer = self.conn.get("CUSTOMER_"+str(c_id)+"_"+str(c_d_id)+"_"+str(c_w_id))
            selected_customer = return_columns_single_record(["C_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DATA"], customer, "CUSTOMER")
            #c_balance = selected_customer[14]
            #c_ytd_payment = selected_customer[15]
            #c_payment_cnt = selected_customer[16]
            #c_data = selected_customer[17]
            
            updated_customer = self.increment_field("C_BALANCE", -1 * h_amount, customer, "CUSTOMER")
            updated_customer = self.increment_field("C_YTD_PAYMENT", h_amount, updated_customer, "CUSTOMER")
            updated_customer = self.increment_field("C_PAYMENT_CNT", 1, updated_customer, "CUSTOMER")
            self.conn.set("CUSTOMER_"+str(c_id)+"_"+str(c_d_id)+"_"+str(c_w_id), updated_customer)
        
        #-------------------------------------------------------------------------------------
        # Case 2, the customer is selected based on customer last name: all rows in the CUSTOMER table with matching 
        # C_W_ID, C_D_ID and C_LAST are selected sorted by C_FIRST in ascending order. Let n be the number of rows selected.  
        # C_ID, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, and C_BALANCE are retrieved from the row at position (n/2 rounded up to the next integer) 
        # in the sorted set of selected rows from the CUSTOMER table. C_BALANCE is decreased by H_AMOUNT. 
        # C_YTD_PAYMENT is increased by H_AMOUNT. C_PAYMENT_CNT is incremented by 1.
        #-------------------------------------------------------------------------------------
        
        else:
        
            all_customers= []
            for idx in range(1, MAX_CUSTOMER_ID+1):
                customer_record = self.conn.get("CUSTOMER_"+str(idx)+"_"+str(c_d_id)+"_"+str(c_w_id))
                if customer_record != None:
                    all_customers.append(customer_record)
            
            filter_customer_table = filter_table([["C_LAST",c_last]],all_customers,"CUSTOMER")
            ordered_customer_map = self.order_by("C_FIRST", filter_customer_table, "CUSTOMER");
            
            # Get the midpoint customer's id
            namecnt = len(filter_customer_table)
            assert namecnt > 0
            index = ordered_customer_map[(namecnt-1)/2][0]
            customer = filter_customer_table[index]

            columns = TABLE_COLUMNS["CUSTOMER"]
            c_index =  columns.index('C_ID')

            c_id = customer[c_index]

            selected_customer = return_columns_single_record(["C_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DATA"], customer, "CUSTOMER")

            updated_customer = self.increment_field("C_BALANCE", -1* h_amount, customer, "CUSTOMER")
            updated_customer = self.increment_field("C_YTD_PAYMENT", h_amount, updated_customer, "CUSTOMER")
            updated_customer = self.increment_field("C_PAYMENT_CNT", 1, updated_customer, "CUSTOMER")
            self.conn.set("CUSTOMER_"+str(c_id)+"_"+str(c_d_id)+"_"+str(c_w_id), updated_customer)

        assert len(customer) > 0
        c_data = selected_customer[17]
        #-------------------------------------------------------------------------------------
        # If the value of C_CREDIT is equal to "BC", then C_DATA is also retrieved from the selected customer and 
        # the following history information: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are inserted at the 
        # left of the C_DATA field by shifting the existing content of C_DATA to the right by an equal number of 
        # bytes and by discarding the bytes that are shifted out of the right side of the C_DATA field. 
        # The content of the C_DATA field never exceeds 500 characters. The selected customer is updated with 
        # the new C_DATA field.
        #-------------------------------------------------------------------------------------
        columns = TABLE_COLUMNS["CUSTOMER"]
        credit_index = columns.index("C_CREDIT")

        if(customer[credit_index] == constants.BAD_CREDIT):
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
        
            updated_customer = self.update_single_record("C_DATA", c_data, updated_customer,"CUSTOMER")
                            
        #-------------------------------------------------------------------------------------
        # H_DATA is built by concatenating W_NAME and D_NAME separated by 4 spaces.
        # A new row is inserted into the HISTORY table with H_C_ID = C_ID, H_C_D_ID = C_D_ID, 
        # H_C_W_ID = C_W_ID, H_D_ID = D_ID, and H_W_ID = W_ID.
        #-------------------------------------------------------------------------------------
        columns = TABLE_COLUMNS["WAREHOUSE"]
        w_index = columns.index("W_NAME")
        columns = TABLE_COLUMNS["DISTRICT"]
        d_index = columns.index("D_NAME")

        h_data = "%s    %s" % (warehouse[w_index], district[d_index])
        value = [ d_id, w_id, h_date, h_amount, h_data ]
        self.conn.set("HISTORY_"+str(c_id)+"_"+str(c_d_id)+"_"+str(c_w_id)+"_"+str(d_id)+"_"+str(w_id),value)
        return [ selected_warehouse, selected_district, updated_customer ]
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        #-------------------------------------------------------------------------------------
        # The row in the DISTRICT table with matching D_W_ID and D_ID is selected and D_NEXT_O_ID is retrieved.
        #-------------------------------------------------------------------------------------
        
        district = self.conn.get("DISTRICT_"+str(d_id)+"_"+str(w_id))
        selected_district = return_columns_single_record(["D_NEXT_O_ID"], district, "DISTRICT")
        next_o_id = selected_district[0]
        #-------------------------------------------------------------------------------------
        # All rows in the ORDER-LINE table with matching OL_W_ID (equals W_ID), OL_D_ID (equals D_ID), 
        # and OL_O_ID (lower than D_NEXT_O_ID and greater than or equal to D_NEXT_O_ID minus 20) are selected. 
        # They are the items for 20 recent orders of the district.
        #-------------------------------------------------------------------------------------

        columns = TABLE_COLUMNS["ORDER_LINE"]
        idx_O_ID = columns.index("OL_O_ID")
        
	# read the customers for the district and warehouse
	"""
        all_customers_idx = []
        for idx in range(1, MAX_CUSTOMER_ID+1):
            customer_record = self.conn.get("CUSTOMER_"+str(idx)+"_"+str(d_id)+"_"+str(w_id))
	    if customer_record != None:
            	all_customers_idx.append(idx)
	"""

        # read all the orders for the customers
        all_orders = []
        for idx in range(1,MAX_CUSTOMER_ID+1):
            order_ids_str = self.conn.get("ORDERS_ID_"+str(idx)+"_"+str(d_id)+"_"+str(w_id))
            if order_ids_str != None:
                order_ids = order_ids_str.split(":");
                for j in range(len(order_ids)-1):
                    o_id = int(order_ids[j])
                    order_record = self.conn.get("ORDER_LINE_"+str(o_id)+"_"+str(d_id)+"_"+str(w_id))
                    if order_record != None:
                        if order_record[idx_O_ID] < next_o_id or order_record[idx_O_ID] >= (next_o_id - 20):
                            all_orders.append(order_record)

        assert all_orders  
        order_lines_i = return_columns(["OL_O_ID"], all_orders, "ORDER_LINE")

        #-------------------------------------------------------------------------------------
        # All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals W_ID) 
        # from the list of distinct item numbers and with S_QUANTITY lower than threshold are counted (giving low_stock).
        #-------------------------------------------------------------------------------------
        
        all_stock = []
        for idx in order_lines_i:
            idx = idx[0]
            stock = self.conn.get("STOCK_"+str(idx)+"_"+str(w_id))
            if stock != None:
                quantity = return_columns_single_record(["S_QUANTITY"], stock, "STOCK")
                if quantity[0] < threshold  :
                    all_stock.append(stock)
       
        assert all_stock 
        if len(all_stock) ==1:
            stock_i = return_columns_single_record(["S_I_ID"],all_stock,"STOCK")
        else:
            stock_i = return_columns(["S_I_ID"], all_stock, "STOCK")

        stock_i = self.count_distinct_values(stock_i)
        return [ stock_i ]

    ## ----------------------------------------------
    ## Update single record
    ## ----------------------------------------------    
    
    def update_single_record(self,field, value, record, table_name):
        columns = TABLE_COLUMNS[table_name] 
        field_index  = columns.index(field)
        record[field_index] = value
        return record
    
    
    ## ----------------------------------------------
    ## increment_field
    ## ----------------------------------------------    
    def increment_field(self,field, value, record, table_name):
        columns = TABLE_COLUMNS[table_name] 
        field_index  = columns.index(field)
    
        record[field_index] += value
        return record
    
    ## ----------------------------------------------
    ## count_distinct_values
    ## ----------------------------------------------    
    def count_distinct_values(self,values):
        value_map={}
        for val in values:
            value_map[val[0]]=1

        return len(value_map)
    
    ## ----------------------------------------------
    ## order_by
    ## ----------------------------------------------    
    #order_by("C_FIRST", filtered_customer_table, "CUSTOMER");
    
    def order_by(self,field,records,table_name):
        columns = TABLE_COLUMNS[table_name] 
        field_index  = columns.index(field)
        #filter_records = return_columns([field],records)
        orderby_map={}
        for idx,tvalue in irange(records):
            field_value = tvalue[field_index]
            orderby_map[idx] = field_value
    
        import operator
        sorted_index_list = sorted(orderby_map.items(), key=operator.itemgetter(1),reverse=False)    
        
        return sorted_index_list
        #store index, value & then sort...
