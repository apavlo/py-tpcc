# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Marcelo Martins
# http://www.cs.brown.edu/~martins/
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
from abstractdriver import *
from pprint import pprint, pformat
from pyrant import protocol

import constants
import logging
import os
import pyrant
import sys

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
		"W_STREEET_2", # VARCHAR
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
	],
	constants.TABLENAME_ORDERS: [
		"O_ID", # INTEGER
		"O_C_ID", # INTEGER
		"O_D_ID", # TINYINT
		"O_W_ID", # SMALLINT
		"O_ENTRY_ID", # TIMESTAMP
		"O_CARRIER_ID", # INTEGER
		"O_OL_CNT", # INTEGER
		"O_ALL_LOCAL", # INTEGER
	],
	constants.TABLENAME_NEW_ORDER: [
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
	constants.TABLENAME_HISTORY: [
		"H_C_ID", # INTEGER
		"H_C_D_ID", # TINYINT
		"H_C_W_ID", # SMALLINT
		"H_D_ID", # TINYINT
		"H_W_ID", # SMALLINT
		"H_DATA", # TIMESTAMP
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
	constants.TABLENAME_CUSTOMER: [
		"C_ID",
		"C_D_ID",
		"C_W_ID",
	],
	constants.TABLENAME_STOCK: [
		"S_I_ID",
		"S_W_ID",
	],
	constants.TABLENAME_ORDERS: [
		"O_ID",
		"O_D_ID",
		"O_W_ID",
		"O_C_ID",
	],
	constants.TABLENAME_NEW_ORDER: [
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
## TokyocabinetDriver
## ==============================================
class TokyocabinetDriver(AbstractDriver):

	## Tokyo Tyrant provides one connection per *table*, not per database.
	
	## Config files have no hierarchy. Let's set up our own hierarchy as a
	## stringfied dictionary and evaluate it later.

	## Each table connection defines the host and port location of the Tokyo
	## Tyrant server

	DEFAULT_CONFIG = { "servers": ("Tokyo Tyrant server configuration", '{ 0: { "' + 
		constants.TABLENAME_ITEM + '":{ "host": "localhost", "port": 1978, },"' +
		constants.TABLENAME_WAREHOUSE + '" : { "host": "localhost", "port": 1979, },"' +
		constants.TABLENAME_DISTRICT + '" : { "host": "localhost", "port": 1980, },"' +
		constants.TABLENAME_CUSTOMER + '" : { "host": "localhost", "port": 1981, },"' +
		constants.TABLENAME_STOCK + '" : { "host": "localhost", "port": 1982, },"' +
		constants.TABLENAME_ORDERS + '" : { "host": "localhost", "port": 1983, },"' +
		constants.TABLENAME_NEW_ORDER + '" : { "host": "localhost", "port": 1984, },"' +
		constants.TABLENAME_ORDER_LINE+ '" : { "host": "localhost", "port": 1985, },"' +
		constants.TABLENAME_HISTORY + '" : { "host": "localhost",	"port": 1986, }, }, }' ), }

	def __init__(self, ddl):
		super(TokyocabinetDriver, self).__init__("tokyocabinet", ddl)
		self.databases = dict()
		self.conn = dict()
		self.numServers = 0

	##-----------------------------------------------
	## self.tupleToString
	##-----------------------------------------------
	def tupleToString(self, tuple, sep=":"):
		"""Tokyo-Cabinet table-type databases only accept strings as keys.
		   This function transforms a compound key (tuple) into a string.
		   Tuples elements are separated by the sep char"""
		return sep.join(str(t) for t in tuple)

    ##-----------------------------------------------
	## self.getServer
	##-----------------------------------------------
	def getServer(self, warehouseID):
		"""Tokyo Cabinet does not support data partitioning. For the TPC-C
		benchmark, we manually partition data based on the warehouse ID"""
		return (warehouseID % self.numServers)

	## ----------------------------------------------
	## makeDefaultConfig
	## ----------------------------------------------
	def makeDefaultConfig(self):
		return TokyocabinetDriver.DEFAULT_CONFIG

	## ----------------------------------------------
	## loadConfig
	## ----------------------------------------------
	def loadConfig(self, config):
		for key in TokyocabinetDriver.DEFAULT_CONFIG.keys():
			assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)

		if config["servers"]:
			# Whn reading from INI file, we need to convert the server
			# description-object from string to a real dictionary
			config["servers"] = eval(config["servers"])
			for serverId, tables in config["servers"].iteritems():
				self.databases[serverId] = tables

		# First connect to databases
		for serverId, tables in self.databases.iteritems():
			self.conn[serverId] = dict()
			for tab, values in tables.iteritems():
				self.conn[serverId][tab] = pyrant.Tyrant(values["host"], values["port"])
		## FOR

		# Remove previous data
		if config["reset"]:
			for serverId, tables in self.conn.iteritems():
				for tab in tables.keys():
					logging.debug("Deleting database '%s' at server '%s'" % (tab, serverId))
					self.conn[serverId][tab].clear()
				## FOR
			## FOR
		## IF

		self.numServers = len(self.databases.keys())
		logging.info("Number of servers: %s" % self.numServers)

	## -------------------------------------------
	## loadTuples
	## -------------------------------------------
	def loadTuples(self, tableName, tuples):
		"""Load tuples into tables of database
		   Each table is a connection to a Tyrant server. Each record is a key-value pair,
		   where key = primary key, values = concatenation of columns (dictionary). If
		   key is compound we transform it into a string, since TC does not support
		   compound keys. Data partitioning occurs based on Warehouse ID."""

		if len(tuples) == 0: return

		logging.debug("Loading %d tuples of tableName %s" % (len(tuples), tableName))

		assert tableName in TABLE_COLUMNS, "Unexpected table %s" % tableName
		columns = TABLE_COLUMNS[tableName]
		num_columns = xrange(len(columns))
		records = list()

		if tableName == constants.TABLENAME_WAREHOUSE:
			for t in tuples:
				w_key = t[0] # W_ID
				sID = self.getServer(w_key)
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((str(w_key), cols))
			## FOR

			try:
				self.conn[sID][tableName].multi_set(records)
			except KeyError, err:
				sys.stderr.write("%s(%s): server ID does not exist or is offline\n" %(KeyError, err))
				sys.exit(1)

		elif tableName == constants.TABLENAME_DISTRICT:
			for t in tuples:
				w_key = t[1] # W_ID
				sID = self.getServer(w_key)
				d_key = self.tupleToString(t[:2]) # D_ID, D_W_ID
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((d_key, cols))
			## FOR

			try:
				self.conn[sID][tableName].multi_set(records)
			except KeyError, err:
				sys.stderr.write("%s(%s): server ID does not exist or is offline\n" %(KeyError, err))
				sys.exit(1)

		## Item table doesn't have a w_id for partition. Replicate it to all
		## servers
		elif tableName == constants.TABLENAME_ITEM:
			for t in tuples:
				i_key = str(t[0])
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((i_key, cols))
			## FOR

			for i in xrange(self.numServers):
				try:
					self.conn[i][tableName].multi_set(records)
				except KeyError, err:
					sys.stderr.write("%s(%s): server ID doesn't exist or is offline\n" %(KeyError, err))
					sys.exit(1)
				## FOR
			## FOR

		elif tableName == constants.TABLENAME_CUSTOMER:
			for t in tuples:
				w_key = t[2] # W_ID
				sID = self.getServer(w_key)
				c_key = self.tupleToString(t[:3]) # C_ID, C_D_ID, C_W_ID
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((c_key, cols))
			## FOR

			try:
				self.conn[sID][tableName].multi_set(records)
			except KeyError, err:
				sys.stderr.write("%s(%s): server ID does not exist or is offline\n" %(KeyError, err))
				sys.exit(1)

		elif tableName == constants.TABLENAME_HISTORY:
			for t in tuples:
				w_key = t[4] # W_ID
				# Not really a primary key, but we need to generate
				# something tobe our key
				h_key = self.tupleToString(t[:3]) # H_C_ID, H_C_D, H_C_W
				sID = self.getServer(w_key)
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((h_key, cols))
			## FOR

			try:
				self.conn[sID][tableName].multi_set(records)
			except KeyError, err:
				sys.stderr.write("%s(%s): server ID does not exist or is offline\n" %(KeyError, err))
				sys.exit(1)

		elif tableName == constants.TABLENAME_STOCK:
			for t in tuples:
				w_key = t[1] # W_ID
				sID = self.getServer(w_key)
				s_key = self.tupleToString(t[:2]) # S_ID, S_W_ID
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((s_key, cols))
			## FOR

			try:
				self.conn[sID][tableName].multi_set(records)
			except KeyError, err:
				sys.stderr.write("%s(%s): server ID does not exist or is offline\n" %(KeyError, err))
				sys.exit(1)

		elif tableName == constants.TABLENAME_ORDERS:
			for t in tuples:
				w_key = t[3] # W_ID
				sID = self.getServer(w_key)
				o_key = self.tupleToString(t[1:4]) # O_ID, O_D_ID, O_W_ID
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((o_key, cols))
			## FOR

			try:
				self.conn[sID][tableName].multi_set(records)
			except KeyError, err:
				sys.stderr.write("%s(%s): server ID does not exist or is offline\n" %(KeyError, err))
				sys.exit(1)

		elif tableName == constants.TABLENAME_NEW_ORDER:
			for t in tuples:
				w_key = t[2] # W_ID
				sID = self.getServer(w_key)
				no_key = self.tupleToString(t[:3]) # NO_O_ID, NO_D_ID, NO_W_ID
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((no_key, cols))
			## FOR
				
			try:
				self.conn[sID][tableName].multi_set(records)
			except KeyError, err:
				sys.stderr.write("%s(%s): server ID does not exist or is offline\n" %(KeyError, err))
				sys.exit(1)

		elif tableName == constants.TABLENAME_ORDER_LINE:
			for t in tuples:
				w_key = t[2] # W_ID
				sID = self.getServer(w_key)
				ol_key = self.tupleToString(t[:4]) # OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER
				cols = dict(map(lambda i: (columns[i], t[i]), num_columns))
				records.append((ol_key, cols))
			## FOR
				
			try:
				self.conn[sID][tableName].multi_set(records)
			except KeyError, err:
				sys.stderr.write("%s(%s): server ID does not exist or is offline\n" %(KeyError, err))
				sys.exit(1)

		logging.debug("Loaded %s tuples for tableName %s" % (len(tuples), tableName))
		return

	## -------------------------------------------
	## loadFinish
	## -------------------------------------------
	def loadFinish(self):

		conn = dict()

		logging.info("Creating indexes...")
		# Add indexes to database after loading all data
		for serverId, tables in self.databases.iteritems():
			conn[serverId] = dict()
			for tab, connValues in tables.iteritems():
				conn[serverId][tab] = protocol.TyrantProtocol(connValues["host"], connValues["port"])
				for index_name in TABLE_COLUMNS[tab]:
					conn[serverId][tab].add_index(index_name)
			## FOR
		## FOR

		logging.info("Optimizing indexes...")
		# Optimize indexes for faster access
		for serverId, tables in self.databases.iteritems():
			for tab, connValues in tables.iteritems():
				for index_name in TABLE_COLUMNS[tab]:
					conn[serverId][tab].optimize_index(index_name)
			## FOR
		## FOR

		logging.info("Syncing to disk...")
		# Finally, flush everything to disk
		for tab in TABLE_COLUMNS.keys():
			for sID in self.conn.keys():
				self.conn[sID][tab].sync()
		## FOR
				
		logging.info("Finished loading tables")

	## --------------------------------------------
	## doDelivery
	## --------------------------------------------
	def doDelivery(self, params):
		"""Execute DELIVERY Transaction
		Parameters Dict:
			w_id
			o_carrier_id
			ol_delivery_id
		"""

		w_id = params["w_id"]
		o_carrier_id = params["o_carrier_id"]
		ol_delivery_d = params["ol_delivery_d"]

		sID = self.getServer(w_id)

		newOrderQuery = self.conn[sID][constants.TABLENAME_NEW_ORDER].query
		ordersQuery   = self.conn[sID][constants.TABLENAME_ORDERS].query
		orderLineQuery = self.conn[sID][constants.TABLENAME_ORDER_LINE].query
		customerQuery = self.conn[sID][constants.TABLENAME_CUSTOMER].query

		results = [ ]
		for d_id in xrange(1, constants.DISTRICTS_PER_WAREHOUSE+1):

			# getNewOrder
			# SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1
			
			newOrders = newOrderQuery.filter(NO_D_ID = d_id, NO_W_ID = w_id, NO_O_ID__gt = -1).columns("NO_O_ID")
			if len(newOrders) == 0:
				## No orders for this district: skip it. Note: This must
				## reported if > 1%
				continue
			assert(newOrders) > 0
			no_o_id = int(newOrders[0]["NO_O_ID"])

			# sumOLAmount
			# SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?

			olines = orderLineQuery.filter(OL_O_ID = no_o_id, OL_D_ID = d_id, OL_W_ID = w_id).columns("OL_AMOUNT")

			# These must be logged in the "result file" according to TPC-C 
			# 2.7.22 (page 39)
			# We remove the queued time, completed time, w_id, and 
			# o_carrier_id: the client can figure them out
			# If there are no order lines, SUM returns null. There should
			# always be order lines.
			assert len(olines) > 0, "ol_total is NULL: there are no order lines. This should not happen"

			ol_total = sum(float(i["OL_AMOUNT"]) for i in olines)

			assert ol_total > 0.0

			# deleteNewOrder
			# DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?

			orders = newOrderQuery.filter(NO_D_ID = d_id, NO_W_ID = w_id, NO_O_ID = no_o_id)
			orders.delete(quick=True)

			# HACK: not transactionally safe
			# getCId
			# SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?

			orders = ordersQuery.filter(O_ID = no_o_id, O_D_ID = d_id, O_W_ID = w_id)
			assert len(orders) > 0
			c_id = int(orders.columns("O_C_ID")[0]["O_C_ID"])

			# updateOrders
			# UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?

			records = list()
			for record in orders:
				key, cols = record
				cols["O_CARRIER_ID"] = o_carrier_id
				records.append((key, cols))
			## FOR

			self.conn[sID][constants.TABLENAME_ORDERS].multi_set(records)

			# updateOrderLine
			# UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?

			orders = orderLineQuery.filter(OL_O_ID = no_o_id, OL_D_ID = d_id, OL_W_ID = w_id)
			records = list()
			for record in orders:
				key, cols = record
				cols["OL_DELIVERY_D"] = ol_delivery_d
				records.append((key, cols))
			## FOR

			self.conn[sID][constants.TABLENAME_ORDER_LINE].multi_set(records)

			# updateCustomer
			# UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?

			customers = customerQuery.filter(C_ID = c_id, C_D_ID = d_id, C_W_ID = w_id)
			records = list()
			for record in customers:
				key, cols = record
				cols["C_BALANCE"] = float(cols["C_BALANCE"]) + ol_total
				records.append((key, cols))
			## FOR

			self.conn[sID][constants.TABLENAME_CUSTOMER].multi_add(records)
		
			results.append((d_id, no_o_id))
		## FOR

		return results

	def doNewOrder(self, params):
		"""Execute NEW_ORDER Transaction
		Parameters Dict:
			w_id
			d_id
			c_id
			o_entry_id
			i_ids
			i_w_ids
			i_qtys
		"""

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

		sID = self.getServer(w_id)

		warehouseQuery = self.conn[sID][constants.TABLENAME_WAREHOUSE].query
		districtQuery  = self.conn[sID][constants.TABLENAME_DISTRICT].query
		customerQuery  = self.conn[sID][constants.TABLENAME_CUSTOMER].query
		orderQuery = self.conn[sID][constants.TABLENAME_ORDERS].query
		newOrderQuery = self.conn[sID][constants.TABLENAME_NEW_ORDER].query
		itemQuery = self.conn[sID][constants.TABLENAME_ITEM].query

		all_local = True
		items = [ ]
		for i in xrange(len(i_ids)):
			## Determine if this is an all local order or not
			all_local = all_local and i_w_ids[i] == w_id
			# getItemInfo
			retItems = itemQuery.filter(I_PRICE = i_ids[i])
			if len(items) > 0:
				items.append(retItems.columns())
			else:
				items.append([])
		assert len(items) == len(i_ids)

		## TPCC define 1% of neworder gives a wrong itemid, causing rollback.
		## Note that this will happen with 1% of transactions on purpose.
		for item in items:
			if len(item) == 0:
				## TODO Abort here!
				return
		## FOR

		## -----------------
		## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
		## -----------------
		
		# getWarehouseTaxRate
		# SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?

		taxes = warehouseQuery.filter(W_ID = w_id)
		w_tax = float(taxes.columns("W_TAX")[0]["W_TAX"])

		# getDistrict
		# SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?

		districts = districtQuery.filter(D_ID = d_id, D_W_ID = w_id)
		districtInfo = districts.columns("D_TAX", "D_NEXT_O_ID")[0]
		d_tax = float(districtInfo["D_TAX"])
		d_next_o_id = int(districtInfo["D_NEXT_O_ID"])

		# incrementNextOrderId
		# HACK: This is not transactionally safe!
		# UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?
		records = list()
		for record in districts:
			key, cols = record
			cols["D_NEXT_O_ID"] = d_next_o_id + 1
			records.append((key, cols))
		## FOR

		self.conn[sID][constants.TABLENAME_DISTRICT].multi_set(records)

		# getCustomer
		# SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?

		customers = customerQuery.filter(C_W_ID = w_id, C_D_ID = d_id, C_ID = c_id)
		customerInfo = customers.columns("C_DISCOUNT", "C_LAST", "C_CREDIT")[0]
		c_discount = float(customerInfo["C_DISCOUNT"])

		## -----------------
		## Insert Order Information
		## -----------------
		ol_cnt = len(i_ids)
		o_carrier_id = constants.NULL_CARRIER_ID

		# createOrder
		# INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT,
		#	  O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)

		key = self.tupleToString((d_next_o_id, d_id, w_id))
		cols = {"O_ID": d_next_o_id, "O_D_ID": d_id, "O_W_ID": w_id, "O_C_ID":
						c_id, "O_ENTRY_D": o_entry_d, "O_CARRIER_ID":
						o_carrier_id, "O_OL_CNT": o_ol_cnt, "O_ALL_LOCAL":
						all_local}
		self.conn[sID][constants.TABLENAME_ORDERS].multi_set([(key, cols)])

		# createNewOrder
		# INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)

		key = self.tupleToString((d_next_o_id, d_id, w_id))
		cols = {"NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id}
		self.conn[sID][constants.TABLENAME_NEW_ORDER].multi_set([(key, cols)])

		## -------------------------------
		## Insert Order Item Information
		## -------------------------------

		item_data = [ ]
		total = 0
		for i in xrange(len(i_id)):
			ol_number = i+1
			ol_supply_w_id = i_w_ids[i]
			ol_i_id = i_ids[i]
			ol_quantity = i_qtys[i]

			# getItemInfo
			# SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?

			key, itemInfo = items[i]
			i_price = float(itemInfo["I_PRICE"])
			i_name  = itemInfo["I_NAME"]
			i_data  = itemInfo["I_DATA"]

			# getStockInfo
			# SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK
			#	 WHERE S_I_ID = ? AND S_W_ID = ?

			stocks = stockQuery.filter(S_I_ID = ol_i_id, S_W_ID = ol_supply_w_id)
			if len(stocks) == 0:
				logging.warn("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)"
								% (ol_i_id, ol_supply_w_id))
				continue
			stockInfo = stock.columns("S_QUANTITY", "S_DATA", "S_YTD", "S_ORDER_CNT",
						"S_REMOTE_CNT", "S_DIST_%02d"%d_id)[0]

			s_quantity = int(stockInfo["S_QUANTITY"])
			s_ytd = float(stockInfo["S_YTD"])
			s_order_cnt = int(stockInfo["S_ORDER_CNT"])
			s_remote_cnt = int(stockInfo["S_REMOTE_CNT"])
			s_data = stockInfo["S_DATA"]
			s_dist_xx = stockInfo["S_DIST_%02d"%d_id] 	# Fetches data from the
									# s_dist_[d_id] column

			# updateStock
			# UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ?
			# 	WHERE S_I_ID = ? AND S_W_ID = ?

			s_ytd += ol_quantity
			if s_quantity >= ol_quantity + 10:
				s_quantity = s_quantity - ol_quantity
			else:
				s_quantity = s_quantity + 91 - ol_quantity
			s_order_cnt += 1

			if ol_supply_w_id != w_id: s_remote_cnt += 1

			sSID = self.getServer(ol_supply_w_id)
			stockQuery = self.conn[sID][constants.TABLENAME_STOCK].query
			stocks = stockQuery.filter(S_I_ID = ol_i_id, S_W_ID = ol_supply_w_id)
			records = list()
			for record in stocks:
				key, cols = record
				cols["S_QUANTITY"] = s_quantity
				cols["S_YTD"] = s_ytd
				cols["S_ORDER_CNT"] =  s_order_cnt
				cols["S_REMOTE_CNT"] = s_remote_cnt
				records.append((key, cols))
			## FOR

			self.conn[sSID][constants.TABLENAME_STOCK].multi_set(records)

			if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
				brand_generic = 'B'
			else:
				brand_generic = 'G'

			## Transaction profile states to use "ol_quantity * i_price"
			ol_amount = ol_quantity * i_price
			total += ol_amount

			# createOrderLine
			# INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID,
			#	OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

			key = tupletoString((d_next_o_id, d_id, w_id, ol_number))
			cols = {"OL_O_ID": d_next_o_id, "OL_D_ID": d_id, "OL_W_ID": w_id,
					"OL_NUMBER": ol_number, "OL_I_ID": ol_i_id,
					"OL_SUPPLY_W_ID": ol_supply_w_id, "OL_DELIVERY_D":
					ol_entry_d, "OL_QUANTITY": ol_quantity, "OL_AMOUNT":
					ol_amount, "OL_DIST_INFO": s_dist_xx}
			self.conn[sID][constants.TABLENAME_ORDER_LINE].multi_set([(key, cols)])

			## Add the info to be returned
			item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))
		## FOR
		
		## Adjust the total for the discount
		#print "c_discount:", c_discount, type(c_discount)
		#print "w_tax:", w_tax, type(w_tax)
		#print "d_tax:", d_tax, type(d_tax)
		total *= (1 - c_discount) * (1 + w_tax + d_tax)

		## Pack up values the client is missing (see TPC-C 2.4.3.5)
		misc = [(w_tax, d_tax, d_next_o_id, total)]

		return [ customerInfo, misc, item_data ]

	def doOrderStatus(self, params):
		"""Execute ORDER_STATUS Transaction
		Parameters Dict:
			w_id
			d_id
			c_id
			c_last
		"""
		w_id = params["w_id"]
		d_id = params["d_id"]
		c_id = params["c_id"]
		c_last = params["c_last"]

		assert w_id, pformat(params)
		assert d_id, pformat(params)

		sID = self.getServer(w_id)

		customerQuery = self.conn[sID][constants.TABLENAME_CUSTOMER].query
		orderQuery    = self.conn[sID][constants.TABLENAME_ORDERS].query
		orderLineQuery= self.conn[sID][constants.TABLENAME_ORDER_LINE].query

		if c_id != None:
			# getCustomerByCustomerId
			# SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER
			# 	 WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?

			customers = customerQuery.filter(C_W_ID = w_id, C_D_ID = d_id, C_ID = c_id)
			customerInfo = customers.columns("C_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_BALANCE")[0]
		else:
			# Get the midpoint customer's id
			# getCustomersByLastName
			# SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER
			# 	 WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST

			customers = customerQuery.filter(C_W_ID = w_id, C_D_ID = d_id, C_LAST__contains = c_last).order_by("C_FIRST", numeric=False)
			all_customers = customers.columns("C_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_BALANCE")
			namecnt = len(all_customers)
			assert namecnt > 0
			index = (namecnt-1)/2
			customerInfo = all_customers[index]
			c_id = int(customerInfo["C_ID"])
		assert len(customerInfo) > 0

		# getLastOrder TODO: LIMIT 1
		# SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS
		# 	 WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1

		orders = orderQuery.filter(O_W_ID = w_id, O_D_ID = d_id, O_C_ID = c_id).order_by("-O_ID", numeric=True)
		orderInfo = orders.columns("O_ID", "O_CARRIER_ID", "O_ENTRY_D")[0]

		# getOrderLines
		# SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE
		#	 WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?        
		if len(orders) > 0:
			o_id = int(orderInfo["O_ID"])
			orders = orderLineQuery.filter(OL_W_ID = w_id, OL_D_ID = d_id, OL_O_ID = o_id)
			orderLines = orders.columns("OL_SUPPLY_W_ID", "OL_I_ID", "OL_QUANTITY", "OL_AMOUNT", "OL_DELIVERY_D")
		else:
			orderLines = [ ]

		return [customerInfo, orderInfo, orderLines]

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

		w_id = params["w_id"]
		d_id = params["d_id"]
		h_amount = params["h_amount"]
		c_w_id = params["c_w_id"]
		c_d_id = params["c_d_id"]
		c_id = params["c_id"]
		c_last = params["c_last"]
		h_date = params["h_date"]

		sID = self.getServer(w_id)
		cSID = self.getServer(c_w_id)

		customerQuery  = self.conn[cSID][constants.TABLENAME_CUSTOMER].query
		warehouseQuery = self.conn[sID][constants.TABLENAME_WAREHOUSE].query
		districtQuery  = self.conn[sID][constants.TABLENAME_DISTRICT].query

		if c_id != None:
			# getCustomerByCustomerId
			# SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
			# 	 C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE,
			# 	 C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER
			#	WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?

			customers = customerQuery.filter(C_W_ID = c_w_id, C_D_ID = d_id, C_ID = c_id)
			customerInfo = customers.columns("C_ID", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DATA")[0]
		else:
			# Get the midpoint customer's id
			# getCustomersByLastName
			# SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
			#	 C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE,
			#	 C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER
			#	WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST

			customers = customerQuery.filter(C_W_ID = c_w_id, C_D_ID = d_id, C_LAST__contains = c_last).order_by("C_FIRST", numeric=False)
			all_customers = customers.columns("C_ID", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DATA")
			namecnt = len(all_customers)
			assert namecnt > 0
			index = (namecnt-1)/2
			customerInfo = all_customers[index]
			c_id = int(customerInfo["C_ID"])
		assert len(customerInfo) > 0

		c_balance = float(customerInfo["C_BALANCE"]) - h_amount
		c_ytd_payment = float(customerInfo["C_YTD_PAYMENT"]) + h_amount
		c_payment_cnt = int(customerInfo["C_PAYMENT_CNT"]) + 1
		c_data = customerInfo["C_DATA"]

		# getWarehouse
		# SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?

		warehouses = warehouseQuery.filter(W_ID = w_id)
		warehouseInfo = warehouses.columns("W_NAME","W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP")[0]

		# updateWarehouseBalance
		# UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?

		warehouses = warehouseQuery.filter(W_ID = w_id)
		records = list()
		for record in warehouses:
			key, cols = record
			cols["W_YTD"] = float(cols["W_YTD"]) + h_amount
			records.append((key, cols))
		## FOR
		self.conn[sID][constants.TABLENAME_WAREHOUSE].multi_set(records)

		# getDistrict
		# SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT
		#	 WHERE D_W_ID = ? AND D_ID = ?

		districts = districtQuery.filter(D_W_ID = w_id, D_ID = d_id)
		districtInfo = districts.columns("D_NAME", "D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP")[0]

		# updateDistrictBalance
		# UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?

		districts = districtQuery.filter(W_ID = w_id, D_ID = d_id)
		records = list()
		for record in districts:
			key, cols = record
			cols["D_YTD"] = float(cols["D_YTD"]) + h_amount
			records.append((key, cols))
		## FOR
		self.conn[sID][constants.TABLENAME_DISTRICT].multi_set(records)

		# Customer Credit Information
		customers = customerQuery.filter(C_W_ID = c_w_id, C_D_ID = c_d_id, C_ID = c_id)
		cInfo = customers.columns("C_CREDIT")[0]
		
		if cInfo["C_CREDIT"] == constants.BAD_CREDIT:
			newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
			c_data = (newData + "|" + c_data)
			if len(c_data) > constants.MAX_C_DATA: c_data =	c_data[:constants.MAX_C_DATA]

			# updateBCCustomer
			# UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ?
			#	WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?

			records = list()
			for record in customers:
				key, cols = record
				cols["C_BALANCE"] = c_balance
				cols["C_YTD_PAYMENT"] = c_ytd_payment
				cols["C_PAYMENT_CNT"] = c_payment_cnt
				cols["C_DATA"] = c_data
				records.append((key, cols))
			## FOR
			self.conn[cSID][constants.TABLENAME_CUSTOMER].multi_set(records)
		else:
			c_data = ""

			records = list()
			# updateGCCustomer
			# UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?
			#	WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?

			for record in customers:
				key, cols = record
				cols["C_BALANCE"] = c_balance
				cols["C_YTD_PAYMENT"] = c_ytd_payment
				cols["C_PAYMENT_CNT"] = c_payment_cnt
				records.append((key, cols))
			## FOR
			self.conn[cSID][constants.TABLENAME_CUSTOMER].multi_set(records)

		# Concatenate w_name, four space, d_name
		h_data = "%s    %s" % (warehouseInfo["W_NAME"], districtInfo["D_NAME"])

		# Create the history record
		# INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)

		h_key = self.tupleToString((c_id, c_d_id, c_w_id))

		cols = {"H_C_ID": c_id, "H_C_D_ID": c_d_id, "H_C_W_ID": c_w_id, "H_D_ID":
						d_id, "H_W_ID": w_id, "H_DATE": h_date, "H_AMOUNT":
						h_amount, "H_DATA": h_data}
		self.conn[sID][constants.TABLENAME_HISTORY].multi_set([(key, cols)])

		# TPC-C 2.5.3.3: Must display the following fields:
		# W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY,
		# W_STATE, W_ZIP, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP,
		# C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
		# C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
		# C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT =
		# "BC"), H_AMMOUNT, and H_DATE.

		# Hand back all the warehouse, district, and customer data
		return [ warehouseInfo, districtInfo, customerInfo ]

	def doStockLevel(self, params):
		"""Execute STOCK_LEVEL Transaction
		Parameters Dict:
			w_id
			d_id
			threshold
		"""
		w_id = params["w_id"]
		d_id = params["d_id"]
		threshold = params["threshold"]

		sID = self.getServer(w_id)

		districtQuery  = self.conn[sID][constants.TABLENAME_DISTRICT].query
		orderLineQuery = self.conn[sID][constants.TABLENAME_ORDER_LINE].query
		stockQuery     = self.conn[sID][constants.TABLENAME_STOCK].query

		# getOId
		# "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?"

		districts = districtQuery.filter(D_W_ID = w_id, D_ID = d_id)
		o_id = int(districts.columns("D_NEXT_O_ID")[0]["D_NEXT_O_ID"])

		# getStockCount
		# SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK WHERE OL_W_ID = ? AND OL_D_ID = ?
		# 	AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
      
		orders = orderLineQuery.filter(OL_W_ID = w_id, OL_D_ID = d_id, OL_O_ID__between = [(o_id-20), (o_id-1)]) 
		ol_i_ids = set([i["OL_I_ID"] for i in orders.columns("OL_I_ID")])

		stocks = stockQuery.filter(S_W_ID = w_id, S_I_ID__in = list(ol_i_ids), S_QUANTITY__lt = threshold).columns("S_W_ID")

		cnt = len(stocks)

		return cnt

## CLASS
