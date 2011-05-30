'''
Created on May 2, 2011

@author: Irina Calciu and Alex Gillmor

Scalaris Driver for CS227 TPCC Benchmark
'''

from abstractdriver import *

import os, logging, commands, constants

from collections import defaultdict

from api.Scalaris import JSONConnection, Transaction, TransactionSingleOp, NotFoundException


#Table Definitions
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

def createPrimaryKey(tableName, id, obj):
    '''
    Helper method to create normalized primary keys
    '''
    if tableName == constants.TABLENAME_ITEM:
        return '%s.%s' % (constants.TABLENAME_ITEM, id)
    elif tableName == constants.TABLENAME_WAREHOUSE:
        return '%s.%s' %(constants.TABLENAME_WAREHOUSE, id)
    elif tableName == constants.TABLENAME_DISTRICT:
        return '%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, obj['D_W_ID'], constants.TABLENAME_DISTRICT, id)
    elif tableName == constants.TABLENAME_CUSTOMER:
        return '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, obj['C_W_ID'], \
                                constants.TABLENAME_DISTRICT, obj['C_D_ID'], constants.TABLENAME_CUSTOMER, id)
    elif tableName == constants.TABLENAME_ORDERS:
        return '%s.%s.%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, obj['O_W_ID'], \
                                constants.TABLENAME_DISTRICT, obj['O_D_ID'], constants.TABLENAME_CUSTOMER, obj['O_C_ID'], \
                                constants.TABLENAME_ORDERS, id)
    else:
        return '%s.%s' % (tableName,id)
        
    

    

class ScalarisDriver(AbstractDriver):
    '''
    Scalaris Driver for CS227 TPCC benchmark
    '''
    
    DEFAULT_CONFIG = {
        "database": ("The path to the main Scalaris Node", "http://localhost:8000" ),
    }

    def __init__(self, ddl):
        '''
        init
        '''
        super(ScalarisDriver, self).__init__("scalaris", ddl)
        
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return ScalarisDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in ScalarisDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.database = str(config["database"])
    
            
        self.conn = JSONConnection(url=self.database)
        
        #self.tran = self.transactionFactory()
        #single op is much faster for nearly all operations
        self.tran = TransactionSingleOp(self.conn)
        
    def transactionFactory(self):
        '''
        Transaction object factory method
        '''
        return Transaction(self.conn)
    
    def loadTuples(self, tableName, tuples):
        s = set([constants.TABLENAME_HISTORY, \
                 constants.TABLENAME_NEW_ORDER, \
                 constants.TABLENAME_ORDER_LINE,\
                 constants.TABLENAME_STOCK])
        
        if tableName in s:
            self.loadComplexTuples(tableName,tuples)
        else:
            self.loadSimpleTuples(tableName, tuples)
            
            if tableName == constants.TABLENAME_ORDERS:
                self.loadOrderCustomer(tableName, tuples)
            if tableName == constants.TABLENAME_CUSTOMER:
                self.loadWarehouseDistrictCustomers(tableName, tuples)

    def loadHistory(self,tuples):
        '''
        Specialized method for history table. History is stored based on customer info.
        '''
        history_d=defaultdict(lambda : defaultdict(lambda : defaultdict(list)))
        tableDef = TABLE_COLUMNS[constants.TABLENAME_HISTORY]
        for tuple in tuples:
            history = dict(zip(tableDef,tuple))
            w_id = history["H_C_W_ID"]
            d_id = history['H_C_D_ID']
            c_id = history["H_C_ID"]
            
            history_d[w_id][d_id][c_id].append(history)
        
        for w in history_d.keys():
            for d in history_d[w].keys():
                for o in history_d[w][d].keys():
                    history_key = '%s.%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w,\
                                                    constants.TABLENAME_DISTRICT, d, \
                                                    constants.TABLENAME_CUSTOMER, o,\
                                                    constants.TABLENAME_HISTORY)
                    self.tran.write(history_key, history_d[w][d][o]) 

    def loadStock(self,tuples):
        '''
        Specialized method for laoding stock
        '''

        #need to reestablish because some large values cause problems with scalaris ws
        self.conn = JSONConnection(url=self.database)              
        self.tran = TransactionSingleOp(self.conn)
        
        stock_d = defaultdict(defaultdict)
        tableDef = TABLE_COLUMNS[constants.TABLENAME_STOCK]
        stock_idx = defaultdict(list)
        
        for tuple in tuples:
            stock = dict(zip(tableDef,tuple))
            tuple_short = [tuple[0], tuple[2]]
            stock_short = dict(zip(['S_I_ID', 'S_QUANTITY'],tuple_short))
            s_w_id = stock['S_W_ID']
            s_i_id = stock['S_I_ID']
            stock_d[s_w_id][s_i_id] = stock
            stock_idx[s_w_id].append(stock_short)
        
        for s in stock_d.keys():
            s_key = '%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, s, constants.TABLENAME_STOCK)
            print "key %s" % s_key
            print "value %s" % stock_idx[s]
            self.tran.write(s_key, stock_idx[s])
            
        for w in stock_d.keys():
            for i in stock_d[w].keys():
                s_key = '%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w, constants.TABLENAME_STOCK, i)
                self.tran.write(s_key, stock_d[s][i])
           
           
        
    def loadOrderLine(self,tuples):
        '''
        Load order line objects:
        WAREHOUSE.W_ID.DISTRICT.D_ID.ORDERLINE.O_ID -> OrderLine Object
        WAREHOUSE.W_ID.DISTRICT.D_ID.ORDERLINE -> List of Order Ids for that District
        '''
        ol_d=defaultdict(lambda : defaultdict(lambda : defaultdict(list)))
        order_ids = defaultdict(lambda : defaultdict(list))
        tableDef = TABLE_COLUMNS[constants.TABLENAME_ORDER_LINE]
        for tuple in tuples:
            no = dict(zip(tableDef,tuple))
            w_id = no["OL_W_ID"]
            d_id = no['OL_D_ID']
            o_id = no["OL_O_ID"]
            
            ol_d[w_id][d_id][o_id].append(no)
            order_ids[w_id][d_id].append(str(o_id))
        
        for w in ol_d.keys():
            for d in ol_d[w].keys():
                for o in ol_d[w][d].keys():
                    ol_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w,\
                                                    constants.TABLENAME_DISTRICT, d, \
                                                    constants.TABLENAME_ORDER_LINE, o)
                    self.tran.write(ol_key, ol_d[w][d][o])
        for w in order_ids.keys():
            for d in order_ids[w].keys():
                no_key = '%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w,\
                                                    constants.TABLENAME_DISTRICT, d, \
                                                    constants.TABLENAME_ORDER_LINE)
                self.tran.write(no_key, order_ids[w][d])
    
    def loadNewOrder(self,tuples):
        '''
        Load new order objects:
        WAREHOUSE.W_ID.DISTRICT.D_ID.NEW_ORDER.O_ID -> New Order Object
        WAREHOUSE.W_ID.DISTRICT.D_ID.NEW_ORDER -> List of Order Ids for that new_order
        '''
        no_d=defaultdict(lambda : defaultdict(lambda : defaultdict(list)))
        order_ids = defaultdict(lambda : defaultdict(list))
        tableDef = TABLE_COLUMNS[constants.TABLENAME_NEW_ORDER]
        for tuple in tuples:
            no = dict(zip(tableDef,tuple))
            w_id = no["NO_W_ID"]
            d_id = no['NO_D_ID']
            o_id = no["NO_O_ID"]
            
            no_d[w_id][d_id][o_id].append(no)
            order_ids[w_id][d_id].append(str(o_id))
        
        for w in no_d.keys():
            for d in no_d[w].keys():
                for o in no_d[w][d].keys():
                    no_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w,\
                                                    constants.TABLENAME_DISTRICT, d, \
                                                    constants.TABLENAME_NEW_ORDER, o)
                    self.tran.write(no_key, no_d[w][d][o])

        for w in order_ids.keys():
            for d in order_ids[w].keys():
                no_key = '%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w,\
                                                    constants.TABLENAME_DISTRICT, d, \
                                                    constants.TABLENAME_NEW_ORDER)
                self.tran.write(no_key, order_ids[w][d])
    
    def loadComplexTuples(self, tableName, tuples):
        '''
        Dispatching method for tuples that need secondary/advanced indexing
        '''
        if tableName == constants.TABLENAME_ORDER_LINE:
            self.loadOrderLine(tuples)
        if tableName == constants.TABLENAME_NEW_ORDER:
            self.loadNewOrder(tuples)
        if tableName == constants.TABLENAME_HISTORY:
            self.loadHistory(tuples)
        if tableName == constants.TABLENAME_STOCK:
            self.loadStock(tuples)
        #self.tran.commit()
            
            
    def loadOrderCustomer(self, tableName, tuples):
        '''
        Load order objects:
        WAREHOUSE.W_ID.DISTRICT.D_ID.ORDERS.O_ID -> Order Object
        WAREHOUSE.W_ID.DISTRICT.D_ID.ORDERS-> List of Order Ids for that district
        '''
        
        #order case
        tableDef = TABLE_COLUMNS[tableName]
        o_d=defaultdict(lambda : defaultdict(lambda : defaultdict(list)))
        
        
        for tuple in tuples:
            value = dict(zip(tableDef,tuple))
            o_id = value['O_ID']
            c_id = value['O_C_ID']
            d_id = value['O_D_ID']
            w_id = value['O_W_ID']
            
            oc_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE,w_id, constants.TABLENAME_DISTRICT,d_id, constants.TABLENAME_ORDERS,o_id)
            self.tran.write(oc_key, c_id)
            
            o_d[w_id][d_id][c_id].append(str(o_id))
            
        for k1 in o_d.keys():
            for k2 in o_d[k1].keys():
                for k3 in o_d[k1][k2].keys():
                    orders_key = '%s.%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, k1, constants.TABLENAME_DISTRICT, k2, \
                                            constants.TABLENAME_CUSTOMER, k3, constants.TABLENAME_ORDERS)
                    self.tran.write(orders_key,o_d[k1][k2][k3])
                    
    
    def loadWarehouseDistrictCustomers(self, tableName, tuples):
        '''
        Load warehouse district customers:
        WAREHOUSE.W_ID.DISTRICT.CUSTOMERS -> List of Customer IDs
        '''
        
        #customers
        custs = defaultdict(lambda : defaultdict(list))
        tableDef = TABLE_COLUMNS[tableName]
        for tuple in tuples:
            value = dict(zip(tableDef,tuple)) 
            
            c_last = value['C_LAST']
            c_id = value['C_ID']
            c_idx = {}
            c_idx['C_LAST'] = c_last
            c_idx['C_ID'] = c_id
            d_id = value['C_D_ID']
            w_id = value['C_W_ID']      
            custs[w_id][d_id].append(c_idx)
        
        for w in custs.keys():
            for d in custs[w].keys():
                custs_key = '%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w,\
                                                    constants.TABLENAME_DISTRICT, d, \
                                                    'CUSTOMERS')
                self.tran.write(custs_key, custs[w][d])
 
         
    
    
    def loadSimpleTuples(self, tableName, tuples):
        """Load a list of tuples into the target table"""
    
        self.conn = JSONConnection(url=self.database)              
        self.tran = TransactionSingleOp(self.conn)
        
        if len(tuples) == 0: return
        
        idx =0
        
        tableDef = TABLE_COLUMNS[tableName]
        
        for tuple in tuples:
            pId= tuple[0]
            value = dict(zip(tableDef[1:],tuple[1:]))
            primaryKey = createPrimaryKey(tableName, pId, value)
            self.tran.write(primaryKey, value)
            #self.tran.commit()
            if idx == 500:
                print '%s %s' % (tableName, primaryKey)
                idx = 0 
#                self.tran.commit()
            idx+=1

        #self.tran.commit()

    def loadFinish(self):
        logging.info("Commiting changes to database")
        #self.tran.commit()
        
    def executeStart(self):
        self.conn = JSONConnection(url=self.database)
        
        #self.tran = self.transactionFactory()
        self.tran = TransactionSingleOp(self.conn)
    def executeFinish(self):
        """Callback after the execution phase finishes"""
#        self.tran.commit()
        self.tran.closeConnection()
        #self.tran.commit()
        
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
            ## WAREHOUSE.W_ID.DISTRICT.D_ID.NEW_ORDERS -> List of New Orders 
            no_key = '%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, constants.TABLENAME_NEW_ORDER)
            
            newOrders = None
            newOrders = self.tran.read(no_key) #we expect a list of new order ifd
            if newOrders == None:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            assert len(newOrders) > 0
            
            #just ids            
            no_o_id = min(newOrders)
            newOrders.remove(no_o_id)
            no_o_id = int(no_o_id)
            #new order objects
            
            ## getCId
            ## WAREHOUSE.W_ID.DISTRICT.D_ID.ORDER.ORDER_ID = List of Customers
            customer = None
            c_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                     constants.TABLENAME_DISTRICT, d_id, \
                                     constants.TABLENAME_ORDERS, no_o_id)
            
            customer = self.tran.read(c_key)
            assert customer != None
            c_id = customer

            ## sumOLAmount
            ## WAREHOUSE.W_ID.DISTRICT.D_ID.ORDER_LINE.ORDER_ID -> List of OrderLine Objects or list of OL_NUMBERS
            orderLines = []
            ol_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                     constants.TABLENAME_DISTRICT, d_id, \
                                     constants.TABLENAME_ORDER_LINE, no_o_id)
            
            orderLines = self.tran.read(ol_key)
            
            ol_total = 0.0
            for ol in orderLines:
                ol_total += ol["OL_AMOUNT"]
                ol['OL_DELIVERY_D'] = ol_delivery_d
            ## FOR
            
            ## deleteNewOrder
            #self.new_order.remove({"NO_D_ID": d_id, "NO_W_ID": w_id, "NO_O_ID": no_o_id})
            no_del_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, constants.TABLENAME_NEW_ORDER, no_o_id)
            self.tran.write(no_del_key, None)
            self.tran.write(no_key,newOrders)
            
            ## updateOrders
            order_key = '%s.%s.%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, constants.TABLENAME_CUSTOMER, c_id, constants.TABLENAME_ORDERS, no_o_id)
            order = self.tran.read(order_key)
            order['O_CARRIER_ID'] = o_carrier_id
            self.tran.write(order_key, order)
            #self.orders.update({"O_ID": no_o_id, "O_D_ID": d_id, "O_W_ID": w_id}, {"$set": {"O_CARRIER_ID": o_carrier_id}}, multi=False)
            
            ## updateOrderLine
            #self.order_line.update({"OL_O_ID": no_o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}, {"$set": {"OL_DELIVERY_D": ol_delivery_d}}, multi=False)
            self.tran.write(ol_key, orderLines)
            
            
            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            ## updateCustomer
            customer_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                constants.TABLENAME_DISTRICT, d_id, constants.TABLENAME_CUSTOMER, c_id)
            
            customer = self.tran.read(customer_key)
            customer["C_BALANCE"]=customer["C_BALANCE"]+ol_total
            
            #self.customer.update({"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"$inc": {"C_BALANCE": ol_total}})

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
        
        ## GET ALL ITEMS WITH
        ## ITEM.I_ID
        items = []
        for id in i_ids:
            i_key = '%s.%s' % (constants.TABLENAME_ITEM, id)
            item = self.tran.read(i_key)
            if item:
                items.append(item)
                   
        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        if len(items) != len(i_ids):
            ## TODO Abort here!
            return
        ## IF
        
        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        
        # getWarehouseTaxRate
        ## WAREHOUSE.W_ID -> warehouse object
        w_key = '%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id)
        w = self.tran.read(w_key)
        assert w
        w_tax = w["W_TAX"]
        
        # getDistrict
        ## WAREHOUSE.W_ID.DISTRICT.D_ID -> district object
        
        d_key = '%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id)
        d = self.tran.read(d_key)
        assert d
        d_tax = d["D_TAX"]
        d_next_o_id = d["D_NEXT_O_ID"]
        
        
        # incrementNextOrderId
        d["D_NEXT_O_ID"] = d["D_NEXT_O_ID"] + 1
        self.tran.write(d_key, d);
        
        
        # getCustomer
        ## WAREHOUSE.W_ID.DISTRICT.D_ID.CUSTOMER.C_ID -> Customer Object
        c_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, \
                                       constants.TABLENAME_CUSTOMER, c_id)    
        c = self.tran.read(c_key)
        assert c
        c_discount = c["C_DISCOUNT"]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        
        # createOrder
        ## write to order object to WAREHOUSE.W_ID.DISTRICT.D_ID.CUSTOMER.C_ID.ORDER.O_ID
        o_key = '%s.%s.%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, \
                                       constants.TABLENAME_CUSTOMER, c_id,  constants.TABLENAME_ORDERS, d_next_o_id)
        
        order = {#"O_ID": d_next_o_id, 
                 "O_D_ID": d_id, 
                 "O_W_ID": w_id, 
                 "O_C_ID": c_id, 
                 "O_ENTRY_D": o_entry_d, 
                 "O_CARRIER_ID": o_carrier_id, 
                 "O_OL_CNT": ol_cnt, 
                 "O_ALL_LOCAL": all_local}    
        
        self.tran.write(o_key, order)
   
        
        # createNewOrder
        
        ## write new order object  to WAREHOUSE.W_ID.DISTRICT.D_ID.CUSTOMER.C_ID.ORDER.O_ID.NEW_ORDER.NO_ID
        ## newOrder
        ## assumption
        ##  WAREHOUSE.W_ID.DISTRICT.D_ID.NEW_ORDER.ORDER_ID -> List of New Order Objects
        new_order = {"NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id}
        no_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, constants.TABLENAME_NEW_ORDER, d_next_o_id)
        
        try:
            new_orders = self.tran.read(no_key)
        except NotFoundException:
            new_orders = []
        
        new_orders.append(new_order)
            
        self.tran.write(no_key, new_orders)
        
        #self.new_order.insert({"NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id})

        ## ----------------
        ## OPTIMIZATION:
        ## If all of the items are at the same warehouse, then we'll issue a single
        ## request to get their information
        ## ----------------
        stockInfos = None
        if all_local and False:
            # getStockInfo
            ##WAREHOUSE.W_ID.STOCK -returns-> List of Stock Objects w/ S_I_ID and S_QUANTITY
            allStocks_key = '%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_STOCK)
            allStocks = self.tran.read(allStocks_key)
            
            assert len(allStocks) == ol_cnt
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

            pformat = None
            # getStockInfo
            if all_local and stockInfos != None:
                si = stockInfos[ol_i_id]
                assert si["S_I_ID"] == ol_i_id, "S_I_ID should be %d\n%s" % (ol_i_id, pformat(si))
            else:
                ## WAREHOUSE.W_ID.STOCK.S_ID
                
                allStocks_key = '%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_STOCK)
                allStocks = self.tran.read(allStocks_key)
                
                for stock in allStocks:
                    if stock['S_I_ID'] == ol_i_id:
                        si = stock
                        break
                
            assert si, "Failed to find S_I_ID: %d\n%s" % (ol_i_id, pformat(itemInfo))
            
            si_key = '%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_STOCK, si['S_I_ID'])
            stock= self.tran.read(si_key)
            
            
            s_quantity = si["S_QUANTITY"]
            s_ytd = stock["S_YTD"]
            s_order_cnt = stock["S_ORDER_CNT"]
            s_remote_cnt = stock["S_REMOTE_CNT"]
            s_data = stock["S_DATA"]
            s_dist_xx = stock[s_dist_col] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            
            if ol_supply_w_id != w_id: s_remote_cnt += 1

            # updateStock
            si["S_QUANTITY"] = s_quantity  
            stock["S_QUANTITY"] = s_quantity  
            stock["S_YTD"] = s_ytd 
            stock["S_ORDER_CNT"] = s_order_cnt 
            stock["S_REMOTE_CNT"] = s_remote_cnt 
            ## so this should be cools
            self.tran.write(allStocks_key, allStocks)
            self.tran.write(si_key, stock)
            

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            order_line = {"OL_O_ID": d_next_o_id, 
                          "OL_D_ID": d_id, 
                          "OL_W_ID": w_id, 
                          "OL_NUMBER": ol_number, 
                          "OL_I_ID": ol_i_id,
                           "OL_SUPPLY_W_ID": ol_supply_w_id, 
                           "OL_DELIVERY_D": o_entry_d, 
                           "OL_QUANTITY": ol_quantity, 
                           "OL_AMOUNT": ol_amount,
                           "OL_DIST_INFO": s_dist_xx}

            ##WAREHOUSE.W_ID.DISTRICT.D_ID.ORDER_LINE.ORDER_ID -> List of OrderLine Objects
            ol_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, \
                                            constants.TABLENAME_ORDER_LINE, d_next_o_id)
            
            try:
                order_lines = self.tran.read(ol_key)
            except NotFoundException:
                order_lines = []
            
            order_lines.append(order_line)
            self.tran.write(ol_key, order_lines)

            # createOrderLine
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
        

        if c_id != None:
            # getCustomerByCustomerId
            cust_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                constants.TABLENAME_DISTRICT, d_id, constants.TABLENAME_CUSTOMER, c_id)
            c = self.tran.read(cust_key)
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            #WAREHOUSE.W_ID.DISTRICT.D_ID.CUSTOMERS -> List of Customers (or C_ID:C_LAST pairs)
            all_customers_key = '%s.%s.%s.%s.CUSTOMERS' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                constants.TABLENAME_DISTRICT, d_id)
            
            all_customers = self.tran.read(all_customers_key)
            all_customers = [customer for customer in all_customers if customer['C_LAST']==c_last]
            
            namecnt = len(all_customers)
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
            cust_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                constants.TABLENAME_DISTRICT, d_id, constants.TABLENAME_CUSTOMER, c_id)
            c = self.tran.read(cust_key)

        # getLastOrder
        ## WAREHOUSE.W_ID.DISTRICT.D_ID.CUSTOMER.C_ID.ORDERS - > List of Orders
        orders_key = '%s.%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, \
                                            constants.TABLENAME_CUSTOMER, c_id, constants.TABLENAME_ORDERS)
        orders = self.tran.read(orders_key)
        
        o_id = max(orders)

        order_key = '%s.%s.%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, \
                                            constants.TABLENAME_CUSTOMER, c_id, constants.TABLENAME_ORDERS, o_id)


        order = self.tran.read(order_key)
        
        if order:
            # getOrderLines

            ## WAREHOUSE.W_ID.DISTRICT.D_ID.CUSTOMER.ORDER_LINE.O_ID -> List of Orderline Objects 
            ol_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id, \
                                            constants.TABLENAME_ORDER_LINE, o_id)
            orderLines = self.tran.read(ol_key)
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
            cust_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, c_w_id, \
                                constants.TABLENAME_DISTRICT, c_d_id, constants.TABLENAME_CUSTOMER, c_id)
            c = self.tran.read(cust_key)
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            #WAREHOUSE.W_ID.DISTRICT.D_ID.CUSTOMERS -> List of Customers (or C_ID:C_LAST pairs)
            all_customers_key = '%s.%s.%s.%s.CUSTOMERS' % (constants.TABLENAME_WAREHOUSE, c_w_id, \
                                constants.TABLENAME_DISTRICT, c_d_id)
            
            all_customers = self.tran.read(all_customers_key)
            all_customers = [customer for customer in all_customers if customer['C_LAST']==c_last]
            
            namecnt = len(all_customers)
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
            cust_key = '%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, c_w_id, \
                                constants.TABLENAME_DISTRICT, c_d_id, constants.TABLENAME_CUSTOMER, c_id)
            c = self.tran.read(cust_key)
       
        assert len(c) > 0
        assert c_id != None
        c["C_BALANCE"] = c["C_BALANCE"] - h_amount
        c["C_YTD_PAYMENT"] = c["C_YTD_PAYMENT"] + h_amount
        c["C_PAYMENT_CNT"] = c["C_PAYMENT_CNT"] + 1
        c_data = c["C_DATA"]

        # getWarehouse
        
        ## SCALARIS 
        ## WAREHOUSE.W_ID -> Warehouse Object
        w_key = '%s.%s' % (constants.TABLENAME_WAREHOUSE, c_w_id)
        w = self.tran.read(w_key)
        assert w
        
        # getDistrict
        ## SCALARIS
        ## WAREHOUSE.W_ID.DISTRICT.D_ID - > District Object
        d_key = '%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                constants.TABLENAME_DISTRICT, d_id)
        d = self.tran.read(d_key)
        assert d
        
        # updateWarehouseBalance
        w['W_YTD'] = w['W_YTD'] + h_amount
        self.tran.write(w_key, w)
        
        # updateDistrictBalance
        d['D_YTD'] = d['D_YTD'] + h_amount
        self.tran.write(d_key, d)
        
        # Customer Credit Information
        if c["C_CREDIT"] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            
            c['C_DATA']=c_data


        self.tran.write(cust_key, c)
        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (w["W_NAME"], d["D_NAME"])
        
        # insertHistory
        history = {#"H_C_ID": c_id, 
                   #"H_C_D_ID": c_d_id, 
                   #"H_C_W_ID": c_w_id, 
                   "H_D_ID": d_id, 
                   "H_W_ID": w_id, 
                   "H_DATE": h_date, 
                   "H_AMOUNT": h_amount, 
                   "H_DATA": h_data}
        
        h_key = '%s.%s.%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, c_w_id, \
                                          constants.TABLENAME_DISTRICT, c_d_id, \
                                          constants.TABLENAME_CUSTOMER, c_id,
                                          constants.TABLENAME_HISTORY)
        histories = self.tran.read(h_key)
        if histories == None:
            histories = []
        histories.append(history)
        self.tran.write(h_key, histories)

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
        ## WAREHOUSE.W_ID.DISTRICT.D_ID -returns-> District Object w/ attribute: D_NEXT_O_ID
        ##
        d_key = '%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, constants.TABLENAME_DISTRICT, d_id)
        d = self.tran.read(d_key)
        assert d
        o_id = d["D_NEXT_O_ID"]
        
        ## WAREHOUSE.W_ID.DISTRICT.D_ID.ORDER_LINE.ORDER_ID -returns-> List of Order Line Objects w/ OL_I_ID
        ##
        ol_key = '%s.%s.%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                     constants.TABLENAME_DISTRICT, d_id, \
                                     constants.TABLENAME_ORDER_LINE)

        orderLines = self.tran.read(ol_key)
        assert orderLines
        ol_ids = set()
        for ol in orderLines:
            if ol < o_id and ol >= o_id-20:
                ol_ids.add(ol)
        

        ## WAREHOUSE.W_ID.STOCK -returns-> List of Stock Objects w/ S_I_ID and S_QUANTITY
        s_key = '%s.%s.%s' % (constants.TABLENAME_WAREHOUSE, w_id, \
                                     constants.TABLENAME_STOCK)
        stocks = self.tran.read(s_key)
        result = len([stock for stock in stocks if stock['S_I_ID'] in ol_ids and stock['S_QUANTITY'] < threshold])
        
        return int(result)
        
