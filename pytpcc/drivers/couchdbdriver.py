# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Alex Kalinin
# http://www.cs.brown.edu/~akalinin/
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

import logging
from pprint import pformat

import constants
from abstractdriver import *

import couchdb
from uuid import uuid4

# for parallel view fetching
import threading

# This describes our scheme:
#     db -- the name of the corresponding db in CouchDB (we're using table per database approach)
#     attrs -- attributes from the table (will become keys in JSON documents, one document per row)
#     prim_key -- primary key from the original table (after being concatenated will become _id in the JSON document)
#     distr_key -- defines sharding key (sharding is done in a round-robin manner)
#     indexes -- will become CouchDB views (should be seen as CREATE INDEX in SQL; we maintain secondary indexes this way)
#
# To sum up:
#    -- We use one CouchDB database per table, one JSON document per row, one key/value per column/value.
#    -- Secondary indexes are emulated through CouchDB views.
#
TPCC_SCM = {
    "WAREHOUSE": {
        "db" : "warehouse",
        "attrs" : ["W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP", "W_TAX", "W_YTD"],
        "prim_key" : ["W_ID"],
        "distr_key" : "W_ID",
    },
    "DISTRICT":  {
        "db" : "district",
        "attrs" : ["D_ID", "D_W_ID", "D_NAME", "D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP", "D_TAX",
                   "D_YTD", "D_NEXT_O_ID"],
        "prim_key" : ["D_W_ID", "D_ID"],
        "distr_key" : "D_W_ID",
    },
    "ITEM"    :  {
        "db" : "item",
        "attrs" : ["I_ID", "I_IM_ID", "I_NAME", "I_PRICE", "I_DATA"],
        "prim_key" : ["I_ID"]
    },
    "CUSTOMER":  {
        "db" : "customer",
        "attrs" : ["C_ID", "C_D_ID", "C_W_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2",
                   "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM",
                    "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA"],
        "prim_key" : ["C_W_ID" ,"C_D_ID", "C_ID"],
        "distr_key" : "C_W_ID",
        "indexes"   :
                {
                    "w_d_last" : {
                                "map"   :  """
                                              function(doc) {
                                                  emit([doc.C_W_ID, doc.C_D_ID, doc.C_LAST], doc.C_FIRST);
                                              }
                                           """,
                              },
                }
    },
    "HISTORY" :  {
        "db" : "history",
        "attrs" : ["H_C_ID", "H_C_D_ID", "H_C_W_ID", "H_D_ID", "H_W_ID", "H_DATE", "H_AMOUNT", "H_DATA"],
        "prim_key" : [],
        "distr_key" : "H_C_W_ID",
    },
    "STOCK"   :  {
        "db" : "stock",
        "attrs" : ["S_I_ID", "S_W_ID", "S_QUANTITY", "S_DIST_01", "S_DIST_02", "S_DIST_03", "S_DIST_04", "S_DIST_05",
                   "S_DIST_06", "S_DIST_07", "S_DIST_08", "S_DIST_09", "S_DIST_10", "S_YTD", "S_ORDER_CNT",
                   "S_REMOTE_CNT", "S_DATA"],
        "prim_key" : ["S_W_ID", "S_I_ID"],
        "distr_key" : "S_W_ID",
        "indexes"   :
                {
                    "w_i" : {
                                "map"   :  """
                                              function(doc) {
                                                  emit([doc.S_W_ID, doc.S_I_ID], doc.S_QUANTITY);
                                              }
                                           """,
                              },
                }
    },
    "ORDERS"  :  {
        "db" : "orders",
        "attrs" : ["O_ID", "O_C_ID", "O_D_ID", "O_W_ID", "O_ENTRY_D", "O_CARRIER_ID", "O_OL_CNT", "O_ALL_LOCAL"],
        "prim_key" : ["O_W_ID", "O_D_ID", "O_ID"],
        "distr_key" : "O_W_ID",
        "indexes"   :
                {
                    "w_d_c_o" : {
                                "map"   :  """
                                              function(doc) {
                                                  emit([doc.O_W_ID, doc.O_D_ID, doc.O_C_ID, doc.O_ID], null);
                                              }
                                           """,
                              },
                }
    },
    "NEW_ORDER": {
        "db" : "new_order",
        "attrs" : ["NO_O_ID", "NO_D_ID", "NO_W_ID"],
        "prim_key" : ["NO_D_ID", "NO_W_ID", "NO_O_ID"],
        "distr_key" : "NO_W_ID",
    },
    "ORDER_LINE":{
        "db" : "order_line",
        "attrs" :   ["OL_O_ID", "OL_D_ID", "OL_W_ID", "OL_NUMBER", "OL_I_ID", "OL_SUPPLY_W_ID", "OL_DELIVERY_D",
                     "OL_QUANTITY", "OL_AMOUNT", "OL_DIST_INFO"],
        "prim_key": ["OL_W_ID", "OL_D_ID", "OL_O_ID", "OL_NUMBER"],
        "distr_key" : "OL_W_ID",
        "indexes"   :
                {
                    "o_d_w" : {
                                "map"   :  """
                                              function(doc) {
                                                  emit([doc.OL_O_ID, doc.OL_D_ID, doc.OL_W_ID], doc.OL_AMOUNT);
                                              }
                                           """,
                                "reduce":  """
                                              function(keys, values, rereduce) {
                                                  return sum(values);
                                              }
                                           """,
                              },
                    "o_d_w_i" : {
                                "map"   :  """
                                              function(doc) {
                                                  emit([doc.OL_O_ID, doc.OL_D_ID, doc.OL_W_ID], doc.OL_I_ID);
                                              }
                                           """,
                              },
                }
    },
}

def db_from_table(table_name):
    """
    Converts the name of the table to the corresponding CouchDB database name.
    Note, that CouchDB doesn't like CAPITAL database names.
    """
    return TPCC_SCM[table_name]['db']

def gen_pk_doc(table_name, doc):
    """
    Generate primary key for the row-doc from the table_name.
    It is done by just concatenating all 'prim_key' attributes of the table

    If we don't have a key in the primary table, then we just generate it via uuid4.
    It is usually recommended to generate an id on the client side.
    """
    table_schema = TPCC_SCM[table_name]
    if len(table_schema['prim_key']):
        pk = '_'.join([str(doc[attr]) for attr in table_schema['prim_key']])
    else:
        pk = uuid4().hex

    return pk

def touch_view(db, view_name):
    """
    Touches the 'view_name' view from the given db object.

    The main point here is to make CouchDB actually create the view. Otherwise it would only
    create it on the first query. We don't want that, since that would make things very slow during
    the actual transaction processing!
    """
    logging.debug("HACK: Fetching view '%s' from '%s' with 'limit = 1'" % (view_name, str(db)))
    # the result is unimportant here, just use limit=1
    db.view('tpcc/%s' % view_name, limit = 1).rows
    logging.debug("HACK: Fetched view '%s' from '%s' with 'limit = 1'" % (view_name, str(db)))

class TouchThread(threading.Thread):
    """
    This is a class to handle "touch-view" threads, which
    are used to initialize views in the loadFinish function

    The main scheme here is that in case of several shards, we want to fetch the view from all
    the shards simultaneously. 'n' shards equals 'n' threads.

    So, the thread just executes 'touch_view' function and then quits.
    """
    def __init__(self, *args):
        self._target = touch_view
        self._args = args
        threading.Thread.__init__(self)

    def run(self):
        self._target(*self._args)

## ==============================================
## CouchdbDriver
## ==============================================
class CouchdbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "node_urls": ("CouchDB URL:", '["http://localhost:5984"]'), # usual "out-of-the-box" value
    }

    def __init__(self, ddl):
        super(CouchdbDriver, self).__init__("couchdb", ddl)
        self.servers = [] # list of shards (couchdb server objects)
        self.dbs = None   # dict: 'db_name' -> (list of db_obj (shards))

    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return CouchdbDriver.DEFAULT_CONFIG

    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in CouchdbDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)

        # open servers
        for srv_name in eval(config["node_urls"]):
            logging.debug("Got a CouchDB node from config: '%s'" % srv_name)
            # we use delayed commits here since we don't care much about durability
            # note, that couchdb would commit the data once per several seconds anyway
            self.servers.append(couchdb.Server(url = srv_name, full_commit = False))

        db_names = [db_from_table(table) for table in TPCC_SCM.keys()]

        # delete the dbs if we're resetting
        if config["reset"]:
            for db in db_names:
                for srv in self.servers:
                    if db in srv:
                        logging.debug("Deleting database '%s' on server '%s'" % (db, str(srv)))
                        srv.delete(db)

        # creating databases
        self.dbs = dict()
        for db in db_names:
            sdb = [] # list of shards for the db
            for srv in self.servers:
                if not db in srv:
                    logging.debug("Creating database '%s' on server '%s'" % (db, str(srv)))
                    sdb.append(srv.create(db))
                else:
                    logging.debug("Database exists: '%s', server: '%s'" % (db, str(srv)))
                    sdb.append(srv[db])

            self.dbs[db] = sdb

    ## ----------------------------------------------
    ## tuples_to_docs
    ## ----------------------------------------------
    def shard_from_id(self, key):
        """
        Get the shard number from the key. Key is assumed to be integer.

        Just a dumb round-robin.
        """
        return key % len(self.servers)

    ## ----------------------------------------------
    ## tuples_to_docs
    ## ----------------------------------------------
    def tuples_to_docs(self, table_name, tuples):
        """
        This function converts tuples belonging to the table_name to a list
        of documents suitable for loading into CouchDB database with the name table_name

        This is actually not very well written and takes the most CPU time from the loader.
        However, do we actually care? It's just loading. Fetching the views will probably kill us anyway...
        """
        table_schema = TPCC_SCM[table_name]

        # create list of lists for documents (one list of docs per shard)
        docs = [list() for s in self.servers]
        tuple_len = len(tuples[0])

        assert(tuple_len == len(table_schema['attrs'])), "Number of attributes and the tuple length differ: %s" % table_name

        for tup in tuples:
            doc = dict()

            # generate the doc as a simple dict
            for i, attr in enumerate(table_schema['attrs']):
                doc[attr] = tup[i]

            # determine the shard number we want to put the doc into.
            #
            # we use distr_key for that.
            #
            # if the table doesn't have a distr key, we assume it's
            # replicated over all shard nodes
            #
            # it is assumed that the 'distr_key' is integer
            if TPCC_SCM[table_name].has_key("distr_key"):
                distr_key = int(doc[TPCC_SCM[table_name]["distr_key"]])
                shard = self.shard_from_id(distr_key)
            else:
                shard = -1

            # emulate primary key with "id" or generate a random one
            doc['_id'] = gen_pk_doc(table_name, doc)

            # put the doc to the proper list.
            # '-1' means 'replicate to all'
            if shard != -1:
                docs[shard].append(doc)
            else:
                for l in docs:
                    l.append(doc)

        return docs

    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return

        # create docs for tuples
        docs = self.tuples_to_docs(tableName, tuples)
        db_name = db_from_table(tableName)

        # load all documents in bulk on every node
        for srv_num, srv in enumerate(self.servers):
            if len(docs[srv_num]):
                logging.debug("Loading tuples from the table '%s' into database '%s' on server '%s'" % (tableName, db_name, str(srv)))
                # should we check the result here? we're assuming a fresh load.
                self.dbs[db_name][srv_num].update(docs[srv_num])

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        """
        Creates some additional views to speed-up the execution and commits

        This is the tricky part. We want not only to create indexes (views), but also fetch them. Otherwise,
        CouchDB would do it in a lazy way, during a first query. We don't want that at all!
        """
        view_touch_jobs = []
        for table in TPCC_SCM.keys():
            if 'indexes' in TPCC_SCM[table]:
                for srv_num, srv in enumerate(self.servers):
                    # load the design doc: _design/tpcc
                    try:
                        logging.debug("Creating indexes for '%s' on server '%s'" % (table, str(srv)))
                        cdb = self.dbs[db_from_table(table)][srv_num]
                        design_doc = {'views' : TPCC_SCM[table]['indexes']}
                        cdb['_design/tpcc'] = design_doc
                    except couchdb.http.ResourceConflict:
                        # happens if we have multiple loaders. This is okay. The design doc is still the same.
                        pass
                    finally:
                        for view_name in TPCC_SCM[table]['indexes'].keys():
                            view_touch_jobs.append((cdb, view_name))

        # we want actually to initialize views in parallel on all shard nodes
        # to speed-up loading times
        touch_thread_pool = []
        logging.debug("We have %d views to touch" % len(view_touch_jobs))
        for job in view_touch_jobs:
            t = TouchThread(job[0], job[1])
            t.start()
            touch_thread_pool.append(t)

        logging.debug("Waiting for %d view touchers to finish" % len(touch_thread_pool))
        for t in touch_thread_pool:
            t.join()

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = str(params["ol_delivery_d"])

        # Note, we want to do this cycle ASAP, since we're deleting the 'NEW_ORDER' docs and
        # are very vulnerable to conflicts
        no_o_ids = []
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            while True:
                # fetch any 'NEW_ORDER' doc ('0' as the 'NO_O_ID')
                newOrder = self.dbs[db_from_table('NEW_ORDER')][self.shard_from_id(w_id)].view('_all_docs', limit = 1,
                    include_docs = 'true', startkey = gen_pk_doc('NEW_ORDER', {'NO_D_ID': d_id, 'NO_W_ID': w_id, 'NO_O_ID' : 0})).rows

                # it seems that we might fetch a deleted doc in case there are no more. Nice...
                if newOrder[0]['value'].has_key('deleted') and newOrder[0]['value']['deleted'] == True:
                    logging.debug("No documents: _all_docs returned a deleted one. Skipping...")
                    newOrder = []

                if len(newOrder) == 0:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    break

                newOrder = newOrder[0].doc

                try:
                    self.dbs[db_from_table('NEW_ORDER')][self.shard_from_id(w_id)].delete(newOrder)
                    no_o_ids.append((d_id, newOrder['NO_O_ID']))
                    break
                except couchdb.http.ResourceNotFound:
                    # in case somebody got this order first, try to fetch another one
                    logging.debug('Pessimistic concurrency control: Delete failed: Restarting...')
                    pass
                except couchdb.http.ResourceConflict:
                    # in case somebody got this order first, try to fetch another one
                    logging.debug('Pessimistic concurrency control: Delete failed: Restarting...')
                    pass

            if len(newOrder) == 0:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
        ## FOR

        # Now we're "isolated" from concurrent transactions...
        # We're trying to fetch all info using as least requests as possible
        order_keys = [gen_pk_doc('ORDERS', {'O_ID': no_o_id, 'O_W_ID': w_id, 'O_D_ID': d_id}) for d_id, no_o_id in no_o_ids]
        order_docs = self.dbs[db_from_table('ORDERS')][self.shard_from_id(w_id)].view('_all_docs',
                                include_docs = 'true',
                                keys = order_keys).rows
        order_docs = [od.doc for od in order_docs]

        # use the view for the sum aggregate
        ol_totals = self.dbs[db_from_table('ORDER_LINE')][self.shard_from_id(w_id)].view('tpcc/o_d_w', group = 'true',
            keys = [[no_o_id, d_id, w_id] for d_id, no_o_id in no_o_ids]).rows

        # put the fetched information together for every client
        c_ids = []
        for i in range(len(no_o_ids)):
            # find the total for the current (order, district, warehouse)
            # is there some way to find stuff in a list fast?
            ol_total = filter(lambda x: x.key == [no_o_ids[i][1], no_o_ids[i][0], w_id], ol_totals)[0].value
            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0
            c_ids.append((order_docs[i]['O_C_ID'], no_o_ids[i][0], ol_total))

        # this should be safe. no conflicts...
        for order_doc in order_docs:
            order_doc['O_CARRIER_ID'] = o_carrier_id
        self.dbs[db_from_table('ORDERS')][self.shard_from_id(w_id)].update(order_docs)

        # ditto...
        # we must do the second retrieval from ORDER_LINES, since now we need docs, not aggregates
        order_lines = self.dbs[db_from_table('ORDER_LINE')][self.shard_from_id(w_id)].view('tpcc/o_d_w',
            keys = [[no_o_id, d_id, w_id] for d_id, no_o_id in no_o_ids],
            reduce = 'false',
            include_docs = 'true').rows
        order_lines = [r.doc for r in order_lines]

        for ol in order_lines:
            ol['OL_DELIVERY_D'] = ol_delivery_d

        self.dbs[db_from_table('ORDER_LINE')][self.shard_from_id(w_id)].update(order_lines)

        # again, updating clients may introduce conflicts. another bottleneck....
        for c_id, d_id, ol_total in c_ids:
            while True:
                customer_info = self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].get(gen_pk_doc('CUSTOMER',
                    {'C_W_ID': w_id, 'C_D_ID': d_id, 'C_ID': c_id}))
                customer_info['C_BALANCE'] += ol_total

                try:
                    self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].save(customer_info)
                    break
                except couchdb.http.ResourceConflict:
                    # in case somebody updated the customer first, try again with the new revision
                    logging.debug('Pessimistic concurrency control: Update failed: Restarting...')
                    pass

        result = no_o_ids

        return result

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = str(params["o_entry_d"])
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]

        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True
        items = []

        # retrieve and store info about all the items
        item_data = self.dbs[db_from_table('ITEM')][self.shard_from_id(w_id)].view('_all_docs',
                                include_docs = 'true',
                                keys = [str(i) for i in i_ids]).rows

        for i in range(len(i_ids)):
            ## Determine if this is an all local order or not
            all_local = all_local and i_w_ids[i] == w_id

            # get info about the item from the just retrieved bundle
            # filter is  just for finding an item in a list
            doc = filter(lambda it: it.id == str(i_ids[i]), item_data)[0].doc

            ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
            ## Note that this will happen with 1% of transactions on purpose.
            if doc is None:
                ## TODO Abort here!
                return

            items.append((doc['I_PRICE'], doc['I_NAME'], doc['I_DATA']))
        assert len(items) == len(i_ids)

        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        doc = self.dbs[db_from_table('WAREHOUSE')][self.shard_from_id(w_id)].get(str(w_id))
        w_tax = doc['W_TAX']

        # conflict is possible. this is a bottleneck...
        while True:
            district_info = self.dbs[db_from_table('DISTRICT')][self.shard_from_id(w_id)].get(gen_pk_doc('DISTRICT',
                {'D_ID': d_id, 'D_W_ID': w_id}))
            d_tax = district_info['D_TAX']
            d_next_o_id = district_info['D_NEXT_O_ID']

            district_info['D_NEXT_O_ID'] += 1
            try:
                self.dbs[db_from_table('DISTRICT')][self.shard_from_id(w_id)].save(district_info)
                break
            except couchdb.http.ResourceConflict:
                # want to get a unique order id!
                logging.debug('Pessimistic concurrency control: Update failed: Restarting...')
                pass

        customer_info = self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].get(gen_pk_doc('CUSTOMER',
            {'C_W_ID': w_id, 'C_D_ID': d_id, 'C_ID': c_id}))
        c_discount = customer_info['C_DISCOUNT']

        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        order_line_docs = []

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        item_data = []
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

            # we have potential conflict for every stock
            while True:
                stockInfo = self.dbs[db_from_table('STOCK')][self.shard_from_id(ol_supply_w_id)].get(gen_pk_doc('STOCK',
                    {'S_I_ID': ol_i_id, 'S_W_ID': ol_supply_w_id}))
                s_quantity = stockInfo['S_QUANTITY']
                s_ytd = stockInfo['S_YTD']
                s_order_cnt = stockInfo['S_ORDER_CNT']
                s_remote_cnt = stockInfo['S_REMOTE_CNT']
                s_data = stockInfo['S_DATA']
                s_dist_xx = stockInfo['S_DIST_%02d' % d_id] # Fetches data from the s_dist_[d_id] column

                ## Update stock
                s_ytd += ol_quantity
                if s_quantity >= ol_quantity + 10:
                    s_quantity = s_quantity - ol_quantity
                else:
                    s_quantity = s_quantity + 91 - ol_quantity
                s_order_cnt += 1

                if ol_supply_w_id != w_id: s_remote_cnt += 1

                # update stock
                stockInfo['S_QUANTITY'] = s_quantity
                stockInfo['S_YTD'] = s_ytd
                stockInfo['S_ORDER_CNT'] = s_order_cnt
                stockInfo['S_REMOTE_CNT'] = s_remote_cnt

                try:
                    self.dbs[db_from_table('STOCK')][self.shard_from_id(ol_supply_w_id)].save(stockInfo)
                    break
                except couchdb.http.ResourceConflict:
                    # if somebody had reserved the stock before us, repeat.
                    logging.debug('Pessimistic concurrency control: Update failed: Restarting...')
                    pass


            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            # don't insert the order line right now
            # we'll do it in bulk later
            order_line_row = dict(zip(TPCC_SCM['ORDER_LINE']['attrs'], [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id,
                                        o_entry_d, ol_quantity, ol_amount, s_dist_xx]))
            order_line_row['_id'] = gen_pk_doc('ORDER_LINE', order_line_row)
            order_line_docs.append(order_line_row)

            ## Add the info to be returned
            item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))
        ## FOR

        ## ----------------
        ## Insert Order Information
        ## ----------------
        self.dbs[db_from_table('ORDER_LINE')][self.shard_from_id(w_id)].update(order_line_docs)

        orders_row = dict(zip(TPCC_SCM['ORDERS']['attrs'], [d_next_o_id, c_id, d_id, w_id, o_entry_d, o_carrier_id, ol_cnt, all_local]))
        orders_row['_id'] = gen_pk_doc('ORDERS', orders_row)
        self.dbs[db_from_table('ORDERS')][self.shard_from_id(w_id)].save(orders_row)

        new_order_row = dict(zip(TPCC_SCM['NEW_ORDER']['attrs'], [d_next_o_id, d_id, w_id]))
        new_order_row['_id'] = gen_pk_doc('NEW_ORDER', new_order_row)
        self.dbs[db_from_table('NEW_ORDER')][self.shard_from_id(w_id)].save(new_order_row)

        ## Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        customer_info = [ (customer_info['C_DISCOUNT'], customer_info['C_LAST'], customer_info['C_CREDIT']) ]
        return [ customer_info, misc, item_data ]

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
            customer = self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].get(gen_pk_doc('CUSTOMER',
                {'C_W_ID': w_id, 'C_D_ID': d_id, 'C_ID': c_id}))
        else:
            # Get the midpoint customer's id
            all_customers = self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].view('tpcc/w_d_last',
                key = [w_id, d_id, c_last],
                reduce = 'false').rows
            all_customers.sort(lambda x, y: cmp(x['value'], y['value']))

            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt - 1) / 2
            customer = all_customers[index]
            customer = self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].get(customer['id'])
            c_id = customer['C_ID']
        assert len(customer) > 0
        assert c_id != None

        # get the last order from the customer
        order = self.dbs[db_from_table('ORDERS')][self.shard_from_id(w_id)].view('tpcc/w_d_c_o',
            limit = 1,
            include_docs = 'true',
            startkey = [w_id, d_id, c_id, 'a'], # 'a' is just to give all numbers
            endkey = [w_id, d_id, c_id, -1],
            descending = 'true',
            reduce = 'false').rows

        if len(order) > 0:
            order = order[0].doc
            orderLines = self.dbs[db_from_table('ORDER_LINE')][self.shard_from_id(w_id)].view('tpcc/o_d_w',
                    key = [order['O_ID'], d_id, w_id],
                    reduce = 'false',
                    include_docs = 'true').rows

            orderLines = [(o.doc['OL_SUPPLY_W_ID'], o.doc['OL_I_ID'],
                          o.doc['OL_QUANTITY'], o.doc['OL_AMOUNT'],
                          o.doc['OL_DELIVERY_D']) for o in orderLines]
        else:
            orderLines = []

        customer = (customer['C_ID'], customer['C_FIRST'], customer['C_MIDDLE'], customer['C_LAST'], customer['C_BALANCE'])
        order = (order['O_ID'], order['O_CARRIER_ID'], order['O_ENTRY_D'])
        return [ customer, order, orderLines ]

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
        h_date = str(params["h_date"])

        if c_id != None:
            cus_doc_id = gen_pk_doc('CUSTOMER', {'C_W_ID': w_id, 'C_D_ID': d_id, 'C_ID': c_id})
        else:
            # Get the midpoint customer's id
            all_customers = self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].view('tpcc/w_d_last',
                key = [w_id, d_id, c_last],
                reduce = 'false').rows
            all_customers.sort(lambda x, y: cmp(x['value'], y['value']))

            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt - 1) / 2
            customer = all_customers[index]
            cus_doc_id = customer['id']

        # try to update the customer record. conflicts expected.
        while True:
            customer = self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].get(cus_doc_id)
            assert len(customer) > 0
            c_id = customer['C_ID']

            c_balance = customer['C_BALANCE'] - h_amount
            c_ytd_payment = customer['C_YTD_PAYMENT'] + h_amount
            c_payment_cnt = customer['C_PAYMENT_CNT'] + 1

            # Customer Credit Information
            try:
                if customer['C_CREDIT'] == constants.BAD_CREDIT:
                    c_data = customer['C_DATA']
                    newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
                    c_data = (newData + "|" + c_data)
                    if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
                    customer['C_DATA'] = c_data

                customer['C_BALANCE'] = c_balance
                customer['C_YTD_PAYMENT'] = c_ytd_payment
                customer['C_PAYMENT_CNT'] = c_payment_cnt
                self.dbs[db_from_table('CUSTOMER')][self.shard_from_id(w_id)].save(customer)
                break
            except couchdb.http.ResourceConflict:
                logging.debug('Pessimistic concurrency control: Update failed: Restarting...')
                pass

        # conflicts when updating warehouse record and...
        while True:
            warehouse = self.dbs[db_from_table('WAREHOUSE')][self.shard_from_id(w_id)].get(str(w_id))
            warehouse['W_YTD'] += h_amount

            try:
                self.dbs[db_from_table('WAREHOUSE')][self.shard_from_id(w_id)].save(warehouse)
                break
            except couchdb.http.ResourceConflict:
                # pessimistic concurrency control...
                logging.debug('Pessimistic concurrency control: Update failed: Restarting...')
                pass

        # the district record
        while True:
            district = self.dbs[db_from_table('DISTRICT')][self.shard_from_id(w_id)].get(gen_pk_doc('DISTRICT',
                {'D_ID': d_id, 'D_W_ID': w_id}))
            district['D_YTD'] += h_amount

            try:
                self.dbs[db_from_table('DISTRICT')][self.shard_from_id(w_id)].save(district)
                break
            except couchdb.http.ResourceConflict:
                # pessimistic concurrency control...
                logging.debug('Pessimistic concurrency control: Update failed: Restarting...')
                pass

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (warehouse['W_NAME'], district['D_NAME'])
        # Create the history record
        hist = dict(zip(TPCC_SCM['HISTORY']['attrs'],[c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data]))
        self.dbs[db_from_table('HISTORY')][self.shard_from_id(c_w_id)].save(hist)

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        warehouse = (warehouse['W_NAME'], warehouse['W_STREET_1'], warehouse['W_STREET_2'],
                     warehouse['W_CITY'], warehouse['W_STATE'], warehouse['W_ZIP'])
        district = (district['D_NAME'], district['D_STREET_1'], district['D_STREET_2'],
                    district['D_CITY'], district['D_STATE'], district['D_ZIP'])
        customer = (customer['C_ID'], customer['C_FIRST'], customer['C_MIDDLE'], customer['C_LAST'], customer['C_STREET_1'],
                    customer['C_STREET_2'], customer['C_CITY'], customer['C_STATE'], customer['C_ZIP'], customer['C_PHONE'],
                    customer['C_SINCE'], customer['C_CREDIT'], customer['C_CREDIT_LIM'], customer['C_DISCOUNT'],
                    customer['C_BALANCE'], customer['C_YTD_PAYMENT'], customer['C_PAYMENT_CNT'], customer['C_DATA'])

        # Hand back all the warehouse, district, and customer data
        return [ warehouse, district, customer ]

    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------
    def doStockLevel(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

        result = self.dbs[db_from_table('DISTRICT')][self.shard_from_id(w_id)].get(gen_pk_doc('DISTRICT',
            {'D_ID': d_id, 'D_W_ID': w_id}))
        assert result
        o_id = result['D_NEXT_O_ID']

        # note, that we might get only parts of some orders because of isolation issues with NewOrder on 'D_NEXT_O_ID'
        # I really doubt anything can be done about it
        orderLines = self.dbs[db_from_table('ORDER_LINE')][self.shard_from_id(w_id)].view('tpcc/o_d_w_i',
                startkey = [o_id - 20, d_id, w_id],
                endkey = [o_id - 1, d_id, w_id],
                reduce = 'false').rows

        # 'set' operation in the next line just filters out duplicates
        stock_keys = [[w_id, i_id] for i_id in set([r['value'] for r in orderLines])]
        # do an index scan join!
        stock_items = self.dbs[db_from_table('STOCK')][self.shard_from_id(w_id)].view('tpcc/w_i',
                                keys = stock_keys).rows

        count = 0
        for item in stock_items:
            if item.value < threshold:
                count += 1

        return count

## CLASS
