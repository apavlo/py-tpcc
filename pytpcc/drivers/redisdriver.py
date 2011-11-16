import os, redis, time, sys
from datetime import datetime
from pprint import pprint,pformat
from abstractdriver import *

#----------------------------------------------------------------------------
# Redis TPC-C Driver
#
# @author Christopher Keith <Christopher_Keith@brown.edu>
# @author James Tavares
#----------------------------------------------------------------------------
class RedisDriver(AbstractDriver):
	
	KEY_SEPARATOR = ':'
	
	TABLES = {
		'WAREHOUSE' : {
			'columns' : 
				[
					'W_ID',
					'W_NAME',
					'W_STREET_1',
					'W_STREET_2',
					'W_CITY',
					'W_STATE',
					'W_ZIP',
					'W_TAX',
					'W_YTD',
				],
			'map' : 
				{
					'W_ID'       : 0,
					'W_NAME'     : 1,
					'W_STREET_1' : 2,
					'W_STREET_2' : 3,
					'W_CITY'     : 4,
					'W_STATE'    : 5,
					'W_ZIP'      : 6,
					'W_TAX'      : 7,
					'W_YTD'      : 8,
				},
			'primary_key' : ['W_ID'],
			'indexes' : [ ], 
		},
		'DISTRICT' : {
			'columns' :
				[
					'D_ID',
					'D_W_ID',
					'D_NAME',
					'D_STREET_1',
					'D_STREET_2',
					'D_CITY',
					'D_STATE',
					'D_ZIP',
					'D_TAX',
					'D_YTD',
					'D_NEXT_O_ID',
				],
			'map' :
				{
					'D_ID'        : 0,
					'D_W_ID'      : 1,
					'D_NAME'      : 2,
					'D_STREET_1'  : 3,
					'D_STREET_2'  : 4,
					'D_CITY'      : 5,
					'D_STATE'     : 6,
					'D_ZIP'       : 7,
					'D_TAX'       : 8,
					'D_YTD'       : 9,
					'D_NEXT_O_ID' : 10,
				},
			'primary_key' : ['D_W_ID', 'D_ID'],
			'indexes' : [ ],
		},
		'ITEM' : {
			'columns' :
				[
					'I_ID',
					'I_IM_ID',
					'I_NAME',
					'I_PRICE',
					'I_DATA',
				],
			'map' :
				{
					'I_ID'    : 0,
					'I_IM_ID' : 1,
					'I_NAME'  : 2,
					'I_PRICE' : 3,
					'I_DATA'  : 4,
				},
			'primary_key' : ['I_ID'],
			'indexes' : [ ]
		},
		'CUSTOMER' : {
			'columns' :
				[
					'C_ID',
					'C_D_ID',
					'C_W_ID',
					'C_FIRST',
					'C_MIDDLE',
					'C_LAST',
					'C_STREET_1',
					'C_STREET_2',
					'C_CITY',
					'C_ZIP',
					'C_PHONE',
					'C_SINCE',
					'C_CREDIT',
					'C_CREDIT_LIM',
					'C_DISCOUNT',
					'C_BALANCE',
					'C_YTD_PAYMENT',
					'C_PAYMENT_CNT',
					'C_DELIVERY_CNT',
					'C_DATA',
				],
			'map' :
				{
					'C_ID'           : 0,
					'C_D_ID'         : 1,
					'C_W_ID'         : 2,
					'C_FIRST'        : 3,
					'C_MIDDLE'       : 4,
					'C_LAST'         : 5,
					'C_STREET_1'     : 6,
					'C_STREET_2'     : 7,
					'C_CITY'         : 8,
					'C_ZIP'          : 9,
					'C_PHONE'        : 10,
					'C_SINCE'        : 11,
					'C_CREDIT'       : 12,
					'C_CREDIT_LIM'   : 13,
					'C_DISCOUNT'     : 14,
					'C_BALANCE'      : 15,
					'C_YTD_PAYMENT'  : 16,
					'C_PAYMENT_CNT'  : 17,
					'C_DELIVERY_CNT' : 18,
					'C_DATA'         : 19,
				},
			'primary_key' : ['C_W_ID', 'C_D_ID', 'C_ID'],
			'indexes' : [ ],
		},
		'HISTORY' : {
			'columns' : 
				[
					'H_C_ID',
					'H_C_D_ID',
					'H_C_W_ID',
					'H_D_ID',
					'H_W_ID',
					'H_DATE',
					'H_AMOUNT',
					'H_DATA',
				],
			'map' : 
				{
					'H_C_ID'   : 0,
					'H_C_D_ID' : 1,
					'H_C_W_ID' : 2,
					'H_D_ID'   : 3,
					'H_W_ID'   : 4,
					'H_DATE'   : 5,
					'H_AMOUNT' : 6,
					'H_DATA'   : 7,
				},
			'primary_key' : [ ],
			'indexes' : [ ],
		},
		'STOCK' : {
			'columns' :
				[
					'S_I_ID',
					'S_W_ID',
					'S_QUANTITY',
					'S_DIST_01',
					'S_DIST_02',
					'S_DIST_03',
					'S_DIST_04',
					'S_DIST_05',
					'S_DIST_06',
					'S_DIST_07',
					'S_DIST_08',
					'S_DIST_09',
					'S_DIST_10',
					'S_YTD',
					'S_ORDER_CNT',
					'S_REMOTE_CNT',
					'S_DATA',
				],
			'map' :
				{
					'S_I_ID'       : 0,
					'S_W_ID'       : 1,
					'S_QUANTITY'   : 2,
					'S_DIST_01'    : 3,
					'S_DIST_02'    : 4,
					'S_DIST_03'    : 5,
					'S_DIST_04'    : 6,
					'S_DIST_05'    : 7,
					'S_DIST_06'    : 8,
					'S_DIST_07'    : 9,
					'S_DIST_08'    : 10,
					'S_DIST_09'    : 11,
					'S_DIST_10'    : 12,
					'S_YTD'        : 13,
					'S_ORDER_CNT'  : 14,
					'S_REMOTE_CNT' : 15,
					'S_DATA'       : 16,
				},
			'primary_key' : ['S_W_ID', 'S_I_ID'],
			'indexes' : [ ],
		},
		'ORDERS' : {
			'columns' : 
				[
					'O_ID',
					'O_D_ID',
					'O_W_ID',
					'O_C_ID',
					'O_ENTRY_D',
					'O_CARRIER_ID',
					'O_OL_CNT',
					'O_ALL_LOCAL',
				],
			'map' : 
				{
					'O_ID'         : 0,
					'O_D_ID'       : 1,
					'O_W_ID'       : 2,
					'O_C_ID'       : 3,
					'O_ENTRY_D'    : 4,
					'O_CARRIER_ID' : 5,
					'O_OL_CNT'     : 6,
					'O_ALL_LOCAL'  : 7,
				},
			'primary_key' : ['O_ID', 'O_C_ID', 'O_D_ID', 'O_W_ID'],
			'indexes' : [ ],
		},
		'NEW_ORDER' : {
			'columns' : 
				[
					'NO_O_ID',
					'NO_D_ID',
					'NO_W_ID',
				],
			'map' :
				{
					'NO_O_ID' : 0,
					'NO_D_ID' : 1,
					'NO_W_ID' : 2,
				},
			'primary_key' : ['NO_D_ID', 'NO_W_ID', 'NO_O_ID'],
			'indexes' : [ ],
		},
		'ORDER_LINE' : {
			'columns' :
				[
					'OL_O_ID',
					'OL_D_ID',
					'OL_W_ID',
					'OL_NUMBER',
					'OL_I_ID',
					'OL_SUPPLY_W_ID',
					'OL_DELIVERY_D',
					'OL_QUANTITY',
					'OL_AMOUNT',
					'OL_DIST_INFO',
				],
			'map' :
				{
					'OL_O_ID'        : 0,
					'OL_D_ID'        : 1,
					'OL_W_ID'        : 2,
					'OL_NUMBER'      : 3,
					'OL_I_ID'        : 4,
					'OL_SUPPLY_W_ID' : 5,
					'OL_DELIVERY_D'  : 6,
					'OL_QUANTITY'    : 7,
					'OL_AMOUNT'      : 8,
					'OL_DIST_INFO'   : 9,
				},
			'primary_key' : ['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER'],
			'indexes' : [ ],
		}
	}
	
	DEFAULT_CONFIG = {
		'databases' : ("List of Redis Hosts", "127.0.0.1:6379"),
		'host-info' : ("Show information about hosts", 'Verbose'),
		'debug-load' : ("Show Loading Information", 'None'),
		'debug-delivery' : ("Show Delivery Performance", 'None'),
		'debug-new-order' : ("Show New Order Performance", 'None'),
		'debug-order-status' : ("Show Order Status Performance", 'None'),
		'debug-payment' : ("Show Payment Performance", 'None'),
		'debug-stock-level' : ("Show Stock Level Performance", 'None'),
	}
	
	#------------------------------------------------------------------------
	# Class constructor
	#
	# @param string ddl (Data Definintion Language)
	#------------------------------------------------------------------------
	def __init__(self, ddl) :
		super(RedisDriver, self).__init__("redis", ddl)
		self.databases = [ ]
		self.t0 = 0
		self.db_count = 0
		self.metadata = None
		self.r_pipes = [ ]
		self.r_sizes = [ ]
		self.w_pipes = [ ]
		self.w_sizes = [ ]
		self.debug = {
			'load'         : 'None',
			'delivery'     : 'None',
			'new-order'    : 'None',
			'order-status' : 'None',
			'payment'      : 'None',
			'stock-level'  : 'None',
		}
		self.hosts = [ ]
	# End __init__()
	
	#------------------------------------------------------------------------
	# Execute TPC-C Delivery Transaction
	#
	# @param dictionary params (transaction parameters)
	#	{
	#		"w_id"          : value,
	#		"o_carrier_id"  : value,
	#		"ol_delivery_d" : value,
	#	}
	#------------------------------------------------------------------------
	def doDelivery(self, params) :
		if self.debug['delivery'] != 'None' :
			print 'TXN DELIVERY STARTING ------------------'
			tt = time.time()
		if self.debug['delivery'] == 'Verbose' :
			t0 = tt
			
		# Initialize input parameters
		w_id = params["w_id"]
		o_carrier_id = params["o_carrier_id"]
		ol_delivery_d = params["ol_delivery_d"]
		
		# Setup Redis pipelining
		node = self.shard(w_id)
		rdr = self.r_pipes[node]
		wtr = self.w_pipes[node]
		
		# Initialize result set
		result = [ ]
		
		#-------------------------
		# Initialize Data Holders
		#-------------------------
		order_key = [ ]
		ol_total = [ ]
		customer_key = [ ]
		ol_counts = [ ]
		no_o_id = [ ]
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
			order_key.append(None)
			ol_total.append(0)
			customer_key.append(None)
			ol_counts.append(0)
			
		#---------------------
		# Get New Order Query
		#---------------------
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
			cursor = d_id - 1
			# Get set of possible new order ids
			index_key = self.safeKey([d_id, w_id])
			rdr.srandmember('NEW_ORDER.INDEXES.GETNEWORDER.' + index_key)
		id_set = rdr.execute()
		
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
			cursor = d_id - 1
			if id_set[cursor] == None :
				rdr.get('NULL_VALUE')
			else :
				rdr.hget('NEW_ORDER.' + str(id_set[cursor]), 'NO_O_ID')
		no_o_id = rdr.execute()
		
		if self.debug['delivery'] == 'Verbose' :
			print 'New Order Query: ', time.time() - t0
			t0 = time.time()
			
		#-----------------------
		# Get Customer ID Query
		#-----------------------
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
			cursor = d_id - 1
			if no_o_id[cursor] == None :
				order_key.insert(cursor, 'NO_KEY')
			else :
				order_key.insert(
					cursor, 
					self.safeKey([w_id, d_id, no_o_id[0]])
				)
			rdr.hget('ORDERS.' + order_key[cursor], 'O_C_ID')
		c_id = rdr.execute()
		
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
			cursor = d_id - 1
			if no_o_id[cursor] == None or c_id[cursor] == None :
				si_key = 'NO_KEY'
			else :
				si_key = self.safeKey([no_o_id[cursor], w_id, d_id])
			rdr.smembers('ORDER_LINE.INDEXES.SUMOLAMOUNT.' + si_key)
		ol_ids = rdr.execute()
		
		if self.debug['delivery'] == 'Verbose' :
			print 'Get Customer ID Query:', time.time() - t0
			t0 = time.time()
			
		#-----------------------------
		# Sum Order Line Amount Query
		#-----------------------------
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
			cursor = d_id - 1
			if no_o_id[cursor] == None or c_id[cursor] == None :
				rdr.get('NULL_VALUE')
			else :
				for i in ol_ids[cursor] :
					rdr.hget('ORDER_LINE.' + str(i), 'OL_AMOUNT')
					ol_counts[cursor] += 1
					
		pipe_results = rdr.execute()
		index = 0
		counter = 0
		
		for ol_amount in pipe_results : 
			counter += 1
			if counter > ol_counts[index] :
				index += 1
				counter = 0
			elif ol_amount != None :
				ol_total[index] += float(ol_amount)
		
		if self.debug['delivery'] == 'Verbose' :
			print 'Sum Order Line Query:', time.time() - t0
			t0 = time.time()
				
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
			cursor = d_id - 1
			if no_o_id[cursor] == None or c_id[cursor] == None :
				## No orders for this district: skip it. 
				## Note: This must be reported if > 1%
				continue
			
			#------------------------
			# Delete New Order Query
			#------------------------
			no_key = self.safeKey([d_id, w_id, no_o_id[cursor]])
			no_si_key = self.safeKey([d_id, w_id])
			wtr.delete('NEW_ORDER.' + no_key)
			wtr.srem('NEW_ORDER.IDS', no_key)
			wtr.srem('NEW_ORDER.INDEXES.GETNEWORDER.' + no_si_key, no_key)
			
			if self.debug['delivery'] == 'Verbose' :
				print 'Delete New Order Query:', time.time() - t0
				t0 = time.time()
				
			#---------------------
			# Update Orders Query
			#---------------------
			wtr.hset(
				'ORDERS.' + order_key[cursor], 
				'W_CARRIER_ID', 
				o_carrier_id
			)
			
			if self.debug['delivery'] == 'Verbose' :
				print 'Update Orders Query:', time.time() - t0
				t0 = time.time()
					
			#-------------------------
			# Update Order Line Query
			#-------------------------
			for i in ol_ids[cursor] :
				wtr.hset(
					'ORDER_LINE.' + str(i),
					'OL_DELIVERY_D', 
					ol_delivery_d
				)
			
			if self.debug['delivery'] == 'Verbose' :
				print 'Update Order Line Query:', time.time() - t0
				t0 = time.time()
				
		#-----------------------
		# Update Customer Query
		#-----------------------
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :	
			cursor = d_id - 1
			if no_o_id[cursor] == None or c_id[cursor] == None :
				rdr.get('NULL_VALUE')
				customer_key.insert(cursor, 'NO_KEY')
			else :
				customer_key.insert(
					cursor,
					self.safeKey([w_id, d_id, c_id[cursor]])
				)
				rdr.hget('CUSTOMER.' + customer_key[cursor], 'C_BALANCE')
		old_balance = rdr.execute()
		
		for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1) :
			cursor = d_id - 1
			if no_o_id[cursor] == None or c_id[cursor] == None :
				continue
			else :
				new_balance = float(old_balance[cursor]) + float(ol_total[cursor])
				wtr.hset(
					'CUSTOMER.' + customer_key[cursor],
					'C_BALANCE', 
					new_balance
				)
				result.append((d_id, no_o_id[cursor]))
		wtr.execute()
		
		if self.debug['delivery'] == 'Verbose' :
			print 'Update Customer Query:', time.time() - t0
		if self.debug['delivery'] != 'None' :
			print 'TXN DELIVERY:', time.time() - tt
			 
		return result
	# End doDelivery()
	
	#------------------------------------------------------------------------
	# Execute TPC-C New Order Transaction
	#
	# @param dictionary params (transaction parameters)
	#	{
	#		'w_id' : value,
	#		'd_id' : value,
	#		'c_id' : value,
	#		'o_entry_d' : value,
	#		'i_ids' : value,
	#		'i_w_ids' : value,
	#		'i_qtys' : value,
	#	}
	#------------------------------------------------------------------------
	def doNewOrder(self, params) :
		if self.debug['new-order'] != 'None' :
			print 'TXN NEW ORDER STARTING -----------------'
			tt = time.time()
		if self.debug['new-order'] == 'Verbose' :
			t0 = tt
			
		# Initialize transaction parameters
		w_id = params["w_id"]
		d_id = params["d_id"]
		c_id = params["c_id"]
		o_entry_d = params["o_entry_d"]
		i_ids = params["i_ids"]
		i_w_ids = params["i_w_ids"]
		i_qtys = params["i_qtys"]

		# Setup Redis pipelining
		node = self.shard(w_id)
		rdr = self.r_pipes[node]
		wtr = self.w_pipes[node]
		
		# Validate transaction parameters
		assert len(i_ids) > 0
		assert len(i_ids) == len(i_w_ids)
		assert len(i_ids) == len(i_qtys)

		# Check if all items are local
		all_local = True
		items = [ ]
		pipe_results = [ ]
		for i in range(len(i_ids)):
			all_local = all_local and i_w_ids[i] == w_id
			rdr.hgetall('ITEM.' + str(i_ids[i]))
		pipe_results = rdr.execute()
		
		for pr in pipe_results :
			if len(pr) > 0 :
				if pr['I_PRICE'] == None and pr['I_NAME'] == None and pr['I_DATA'] == None :
					result = [ ]
				items.append([
					pr['I_PRICE'],
					pr['I_NAME'],
					pr['I_DATA'],
				])
			else :
				items.append([])
				
		assert len(items) == len(i_ids)
		
		## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
		## Note that this will happen with 1% of transactions on purpose.
		for item in items :
			if len(item) == 0 :
				return
		
		#------------------------------------------------------------
		# Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
		#------------------------------------------------------------
		district_key = self.safeKey([w_id, d_id])
		customer_key = self.safeKey([w_id, d_id, c_id])
		
		#------------------------------
		# Get Warehouse Tax Rate Query
		#------------------------------
		rdr.hget('WAREHOUSE.' + str(w_id), 'W_TAX')
		
		if self.debug['new-order'] == 'Verbose' :
			print 'Get Warehouse Tax Rate Query:', time.time() - t0
			t0 = time.time()
			
		#--------------------
		# Get District Query
		#--------------------
		rdr.hgetall('DISTRICT.' + district_key)
		
		if self.debug['new-order'] == 'Verbose' :
			print 'Get District Query:', time.time() - t0
			t0 = time.time()
			
		#--------------------
		# Get Customer Query
		#--------------------
		rdr.hgetall('CUSTOMER.' + customer_key)
		rdr_results = rdr.execute()
		
		w_tax = float(rdr_results[0])
		d_tax = float(rdr_results[1]['D_TAX'])
		d_next_o_id = rdr_results[1]['D_NEXT_O_ID']
		customer_info = rdr_results[2]
		c_discount = float(rdr_results[2]['C_DISCOUNT'])
		
		if self.debug['new-order'] == 'Verbose' :
			print 'Get Customer Query:', time.time() - t0
			t0 = time.time()
			
		#--------------------------
		# Insert Order Information
		#--------------------------
		ol_cnt = len(i_ids)
		o_carrier_id = constants.NULL_CARRIER_ID
		order_key = self.safeKey([w_id, d_id, d_next_o_id])
		new_order_key = self.safeKey([d_next_o_id, w_id, d_id])
		
		#-------------------------------
		# Increment Next Order ID Query
		#-------------------------------
		wtr.hincrby('DISTRICT.' + district_key, 'D_NEXT_O_ID')
		
		if self.debug['new-order'] == 'Verbose' :
			print 'Increment Next Order ID Query:', time.time() - t0
			t0 = time.time()
			
		#--------------------
		# Create Order Query
		#--------------------
		wtr.sadd('ORDERS.IDS', order_key)
		wtr.hmset(
			'ORDERS.' + order_key,
			{
				'O_ID'         : d_next_o_id,
				'O_D_ID'       : d_id,
				'O_W_ID'       : w_id,
				'O_C_ID'       : c_id,
				'O_ENTRY_D'    : o_entry_d,
				'O_CARRIER_ID' : o_carrier_id,
				'O_OL_CNT'     : ol_cnt,
				'O_ALL_LOCAL'  : all_local
			}
		)
		
		if self.debug['new-order'] == 'Verbose' :
			print 'Create Order Query:', time.time() - t0
			t0 = time.time()
			
		# Add index for Order searching
		si_key = self.safeKey([w_id, d_id, c_id])
		wtr.sadd('ORDERS.INDEXES.ORDERSEARCH', si_key)
		
		#------------------------
		# Create New Order Query
		#------------------------
		wtr.sadd('NEW_ORDER.IDS', new_order_key)
		wtr.hmset(
			'NEW_ORDER.' + new_order_key,
			{
				'NO_O_ID' : d_next_o_id,
				'NO_D_ID' : d_id,
				'NO_W_ID' : w_id,
			}
		)
		
		# Add index for New Order Searching
		si_key = self.safeKey([d_id, w_id])
		wtr.sadd('NEW_ORDER.INDEXES.GETNEWORDER', si_key)
		
		if self.debug['new-order'] == 'Verbose' :
			print 'Create New Order Query:', time.time() - t0
			t0 = time.time()
			
		#-------------------------------
		# Insert Order Item Information
		#-------------------------------
		item_data = [ ]
		total = 0
		
		ol_number = [ ]
		ol_quantity = [ ]
		ol_supply_w_id = [ ]
		ol_i_id = [ ]
		i_name = [ ]
		i_price = [ ]
		i_data = [ ]
		stock_key = [ ]
		stock_info = [ ]
		
		for i in range(len(i_ids)) :
			ol_number.append(i + 1)
			ol_supply_w_id.append(i_w_ids[i])
			ol_i_id.append(i_ids[i])
			ol_quantity.append(i_qtys[i])
			
			itemInfo = items[i]
			i_name.append(itemInfo[1])
			i_data.append(itemInfo[2])
			i_price.append(float(itemInfo[0]))
			
			#-----------------------------
			# Get Stock Information Query
			#-----------------------------
			stock_key.append(self.safeKey([ol_supply_w_id[i], ol_i_id[i]]))
			rdr.hgetall('STOCK.' + stock_key[i])		
	 	stock_info = rdr.execute()
		
		for index, si in enumerate(stock_info) :
			if len(si) == 0 :
				continue
			s_quantity = float(si['S_QUANTITY'])
			s_ytd = float(si['S_YTD'])
			s_order_cnt = float(si['S_ORDER_CNT'])
			s_remote_cnt = float(si['S_REMOTE_CNT'])
			s_data = si['S_DATA']
			if len(str(d_id)) == 1 :
				s_dist_xx = si['S_DIST_0' + str(d_id)]
			else :
				s_dist_xx = si['S_DIST_' + str(d_id)]
			
			if self.debug['new-order'] == 'Verbose' :
				print 'Get Stock Information Query:', time.time() - t0
				t0 = time.time()
				
			#--------------------
			# Update Stock Query
			#--------------------
			s_ytd += ol_quantity[index]
			if s_quantity >= ol_quantity[index] + 10 :
				s_quantity = s_quantity - ol_quantity[index]
			else :
				s_quantity = s_quantity + 91 - ol_quantity[index]
			s_order_cnt += 1
			
			if ol_supply_w_id[index] != w_id : s_remote_cnt += 1
			
			wtr.hmset(
				'STOCK.' + stock_key[index],
				{
					'S_QUANTITY'   : s_quantity,
					'S_YTD'        : s_ytd,
					'S_ORDER_CNT'  : s_order_cnt,
					'S_REMOTE_CNT' : s_remote_cnt
				}
			)
						
			if i_data[index].find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
				brand_generic = 'B'
			else:
				brand_generic = 'G'
			
			## Transaction profile states to use "ol_quantity * i_price"
			ol_amount = ol_quantity[index] * i_price[index]
			total += ol_amount
			
			if self.debug['new-order'] == 'Verbose' :
				print 'Update Stock Query:', time.time() - t0
				t0 = time.time()
				
			#-------------------------
			# Create Order Line Query
			#-------------------------
			order_line_key = self.safeKey([w_id, d_id,d_next_o_id, ol_number])
			si_key = self.safeKey([d_next_o_id, d_id, w_id])
			wtr.sadd('ORDER_LINE.IDS', order_line_key)
			wtr.hmset(
				'ORDER_LINE.' + order_line_key,
				{
					'OL_O_ID'          : d_next_o_id,
					'OL_D_ID'          : d_id,
					'OL_W_ID'          : w_id,
					'OL_NUMBER'        : ol_number[index],
					'OL_I_ID'          : ol_i_id[index],
					'OL_SUPPLY_W_ID'   : ol_supply_w_id[index],
					'OL_DELIVERY_D'    : o_entry_d,
					'OL_QUANTITY'      : ol_quantity[index],
					'OL_AMOUNT'        : ol_amount,
					'OL_DISTRICT_INFO' : s_dist_xx
				}
			)
			
			# Create index for Order Line Searching
			wtr.sadd(
				'ORDER_LINE.INDEXES.SUMOLAMOUNT.' + si_key,
				order_line_key
			)
			
			if self.debug['new-order'] == 'Verbose' :
				print 'Create Order Line Query:', time.time() - t0
				t0 = time.time()
				
			## Add the info to be returned
			item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
		## End for i in range(len(i_ids)) :
		
		## Commit!
		wtr.execute()
		
		## Adjust the total for the discount
		total *= (1 - c_discount) * (1 + w_tax + d_tax)
		
		## Pack up values the client is missing (see TPC-C 2.4.3.5)
		misc = [ (w_tax, d_tax, d_next_o_id, total) ]
		
		if self.debug['new-order'] != 'None' :
			print 'TXN NEW ORDER:', time.time() - tt
		return [ customer_info, misc, item_data ]
	
	#------------------------------------------------------------------------
	# Execute TPC-C Do Order Status transaction
	#
	# @param dictionary params (transaction parameters)
	#	{
	#		'w_id'   : value,
	#		'd_id'   : value,
	#		'c_id'   : value,
	#		'c_last' : value,
	#	}
	#------------------------------------------------------------------------
	def doOrderStatus(self, params) :
		if self.debug['order-status'] != 'None' :
			print 'TXN ORDER STATUS STARTING --------------'
			tt = time.time()
		if self.debug['order-status'] == 'Verbose' :
			t0 = tt
			
		# Initialize transactions parameters
		w_id = params["w_id"]
		d_id = params["d_id"]
		c_id = params["c_id"]
		c_last = params["c_last"]
		
		# Initialize Redis pipelining
		node = self.shard(w_id)
		rdr = self.databases[node].pipeline(False)
		wtr = self.databases[node].pipeline(True)
		
		# Validate transaction parameters
		assert w_id, pformat(params)
		assert d_id, pformat(params)
		
		if c_id != None:
			#-----------------------------------
			# Get Customer By Customer ID Query
			#-----------------------------------
			customer_key = self.safeKey([w_id, d_id, c_id])
			rdr.hgetall('CUSTOMER.' + customer_key)
			results = rdr.execute();
			customer = results[0]
			
			if self.debug['order-status'] == 'Verbose' :
				print 'Get Customer By Customer ID Query:', time.time() - t0
				t0 = time.time()
		else:
			#----------------------------------
			# Get Customers By Last Name Query
			#----------------------------------
			si_key = self.safeKey([w_id, d_id, c_last])
			rdr.smembers('CUSTOMER.INDEXES.NAMESEARCH.' + si_key)
			results = rdr.execute();
			customer_id_set = results[0]
			
			customer_ids = [ ]
			for customer_id in customer_id_set :
				rdr.hgetall('CUSTOMER.' + str(customer_id))
				customer_ids.append(str(customer_id))
			
			customers = [ ]
			unsorted_customers = rdr.execute()
			customers.append(unsorted_customers.pop()) 
			for cust in unsorted_customers :
				for index in range(len(customers)) :
					if cust['C_FIRST'] < customers[index]['C_FIRST'] :
						customers.insert(index, cust)
						continue
			
			assert len(customers) > 0
					
			namecnt = len(customers)
			index = (namecnt - 1)/2
			customer = customers[index]
			customer_key = self.safeKey([
				customer['C_W_ID'], 
				customer['C_D_ID'], 
				customer['C_ID']
			])
			c_id = customer['C_ID']
			
			if self.debug['order-status'] == 'Verbose' :
				print 'Get Customers By Last Name Query:', time.time() - t0
				t0 = time.time()
		assert len(customer) > 0
		assert c_id != None
		
		#----------------------
		# Get Last Order Query
		#----------------------
		search_key = self.safeKey([w_id, d_id, c_id])
		rdr.smembers('ORDERS.INDEXES.ORDERSEARCH.' + search_key)
		results = rdr.execute()
		order_id_set = results[0]
		
		if self.debug['order-status'] == 'Verbose' :
			print 'Get Last Order Query:', time.time() - t0
			t0 = time.time()
			
		order = [ ]
		if len(order_id_set) > 0 :
			order_ids = sorted(list(order_id_set))
			order_key = str(order_ids[0])
			rdr.hgetall('ORDERS.' + order_key)
			
			result = rdr.execute()
			order = [
				result[0]['O_ID'],
				result[0]['O_CARRIER_ID'],
				result[0]['O_ENTRY_D'],
			]
			
			#-----------------------
			# Get Order Lines Query
			#-----------------------
			search_key = self.safeKey([order[0], d_id, w_id])
			rdr.smembers('ORDER_LINE.INDEXES.SUMOLAMOUNT.' + search_key)
			results = rdr.execute()
			line_ids = results[0]
			
			for line_id in line_ids : 
				rdr.hgetall('ORDER_LINE.' + str(line_id))
			
			orderLines = [ ]
			results = rdr.execute()
			for r in results :
				orderLines.append([
					r['OL_SUPPLY_W_ID'],
					r['OL_I_ID'],
					r['OL_QUANTITY'],
					r['OL_AMOUNT'],
					r['OL_DELIVERY_D']
				])
				
			if self.debug['order-status'] == 'Verbose' :
				print 'Get Order Lines Query:', time.time() - t0
		else :
			orderLines = [ ]
		
		if self.debug['order-status'] != 'None' :
			print 'TXN ORDER STATUS:', time.time() - tt
			
		return [ customer, order, orderLines ]
	
	#------------------------------------------------------------------------
	# Execute TPC-C Do Payement Transaction
	#
	# @param dictionary params (transaction parameters)
	#	{
	#		'w_id'     : value,
	#		'd_id'     : value,
	#		'h_amount' : value,
	#		'c_w_id'   : value,
	#		'c_d_id'   : value,
	#		'c_id'     : value,
	#		'c_last'   : value,
	#		'h_date'   : value,	
	#	}
	#------------------------------------------------------------------------
	def doPayment(self, params) :
		if self.debug['payment'] != 'None' :
			print 'TXN PAYMENT STARTING -------------------'
			tt = time.time()
		if self.debug['payment'] == 'Verbose' :
			t0 = tt
			
		# Initialize transaction properties
		w_id = params["w_id"]
		d_id = params["d_id"]
		h_amount = params["h_amount"]
		c_w_id = params["c_w_id"]
		c_d_id = params["c_d_id"]
		c_id = params["c_id"]
		c_last = params["c_last"]
		h_date = params["h_date"]
		
		# Initialize Redis pipeline
		node = self.shard(w_id)
		rdr = self.r_pipes[node]
		wtr = self.w_pipes[node]
		
		t0 = time.time()
		if c_id != None:
			#--------------------------
			# Get Customer By ID Query
			#--------------------------
			customer_key = self.safeKey([w_id, d_id, c_id])
			rdr.hgetall('CUSTOMER.' + customer_key)
			results = rdr.execute()
			customer = results[0]
			
			if self.debug['payment'] == 'Verbose' :
				print 'Get Customer By ID Query:', time.time() - t0
				t0 = time.time()
		else:
			#----------------------------------
			# Get Customers By Last Name Query
			#----------------------------------
			si_key = self.safeKey([w_id, d_id, c_last])
			rdr.smembers('CUSTOMER.INDEXES.NAMESEARCH.' + si_key)
			results = rdr.execute()
			customer_id_set = results[0]
			
			customer_ids = [ ]
			for customer_id in customer_id_set :
				rdr.hgetall('CUSTOMER.' + str(customer_id))
				customer_ids.append(str(customer_id))
			
			customers = [ ]
			unsorted_customers = rdr.execute()
			customers.append(unsorted_customers.pop()) 
			for cust in unsorted_customers :
				for index in range(len(customers)) :
					if cust['C_FIRST'] < customers[index]['C_FIRST'] :
						customers.insert(index, cust)
						continue
			
			assert len(customers) > 0
					
			namecnt = len(customers)
			index = (namecnt - 1)/2
			customer = customers[index]
			customer_key = self.safeKey([
				customer['C_W_ID'], 
				customer['C_D_ID'], 
				customer['C_ID']
			])
			c_id = customer['C_ID']
		
			if self.debug['payment'] == 'Verbose' :
				print 'Get Customers By Last Name Query:', time.time() - t0
				t0 = time.time()
				
		assert len(customer) > 0
		assert c_id != None
		
		c_balance = float(customer['C_BALANCE']) - h_amount
		c_ytd_payment = float(customer['C_YTD_PAYMENT']) + h_amount
		c_payment_cnt = float(customer['C_PAYMENT_CNT']) + 1
		c_data = customer['C_DATA']
		
		#---------------------
		# Get Warehouse Query
		#---------------------
		rdr.hgetall('WAREHOUSE.' + str(w_id))
		
		if self.debug['payment'] == 'Verbose' :
			print 'Get Warehouse Query:', time.time() - t0
			t0 = time.time()
			
		#--------------------
		# Get District Query
		#--------------------
		district_key = self.safeKey([w_id, d_id])
		rdr.hgetall('DISTRICT.' + district_key)
		warehouse, district = rdr.execute()
		
		if self.debug['payment'] == 'Verbose' :
			print 'Get District Query:', time.time() - t0
			t0 = time.time()
			
		#--------------------------------
		# Update Warehouse Balance Query
		#--------------------------------
		wtr.set(
			'WAREHOUSE.' + str(w_id) + '.W_YTD', 
			float(warehouse['W_YTD']) + h_amount
		)
		
		if self.debug['payment'] == 'Verbose' :
			print 'Update Warehouse Query:', time.time() - t0
			t0 = time.time()
			
		#-------------------------------
		# Update District Balance Query
		#-------------------------------
		wtr.set(
			'DISTRICT.' + district_key + '.D_YTD',
			float(district['D_YTD']) + h_amount
		)
		
		if self.debug['payment'] == 'Verbose' :
			print 'Update District Balance Query:', time.time() - t0
			t0 = time.time()
			
		if customer['C_CREDIT'] == constants.BAD_CREDIT:
			#----------------------------------
			# Update Bad Credit Customer Query
			#----------------------------------
			newData = " ".join(
				map(
					str, 
					[c_id, c_d_id, c_w_id, d_id, w_id, h_amount]
				)
			)
			
			c_data = (newData + "|" + c_data)
			if len(c_data) > constants.MAX_C_DATA : 
				c_data = c_data[:constants.MAX_C_DATA]
			
			wtr.hmset(
				'CUSTOMER.' + customer_key,
				{
					'C_BALANCE'     : c_balance,
					'C_YTD_PAYMENT' : c_ytd_payment,
					'C_PAYMENT_CNT' : c_payment_cnt,
					'C_DATA'        : c_data,
				}
			)
			
			if self.debug['payment'] == 'Verbose' :
				print 'Update Bad Credit Customer Query:', time.time() - t0
				t0 = time.time()
		else:
			#-----------------------------------
			# Update Good Credit Customer Query
			#-----------------------------------
			wtr.hmset(
				'CUSTOMER.' + customer_key,
				{
					'C_BALANCE'     : c_balance,
					'C_YTD_PAYMENT' : c_ytd_payment,
					'C_PAYMENT_CNT' : c_payment_cnt,
					'C_DATA'        : '',
				}
			)
			if self.debug['payment'] == 'Verbose' :
				print 'Update Good Credit Customer Query:', time.time() - t0
				t0 = time.time()
				
		wtr.execute()	
		
		# Concatenate w_name, four spaces, d_name
		h_data = "%s    %s" % (warehouse['W_NAME'], district['D_NAME'])
		
		#----------------------
		# Insert History Query
		#----------------------
		next_score = self.metadata.get('HISTORY.next_score')
		self.metadata.incr('HISTORY.next_score')
		
		history_key = self.safeKey(next_score)
		wtr.hmset(
			'HISTORY.' + history_key,
			{
				'H_C_ID'   : c_id,
				'H_C_D_ID' : c_d_id,
				'H_C_W_ID' : c_w_id,
				'H_D_ID'   : d_id,
				'H_W_ID'   : w_id,
				'H_DATE'   : h_date,
				'H_AMOUNT' : h_amount,
				'H_DATA'   : h_data,
			}
		)
		
		if self.debug['payment'] == 'Verbose' :
			print 'Insert History Query:', time.time() - t0
			t0 = time.time()
			
		wtr.execute()
		
		
		# TPC-C 2.5.3.3: Must display the following fields:
		# W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY,
		# W_STATE, W_ZIP, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, 	
		# C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
		# C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
		# C_BALANCE, the first 200 characters of C_DATA 
		# (only if C_CREDIT = "BC"), H_AMOUNT, and H_DATE.
		
		# Hand back all the warehouse, district, and customer data
		
		if self.debug['payment'] != 'None' :
			print 'TXN PAYMENT:', time.time() - tt
		
		return [ warehouse, district, customer ]
	
	#------------------------------------------------------------------------
	# Execute TPC-C Stock Level Transaction
	#
	# @param dictionary params (transaction parameters)
	#	{
	#		'w_id'     : value,
	#		'd_id'     : value,
	#		'threshold : value,	
	#	}
	#------------------------------------------------------------------------	
	def doStockLevel(self, params) :
		if self.debug['order-status'] != 'None' :
			print 'TXN STOCK LEVEL STARTING ---------------'
			tt = time.time()
		if self.debug['stock-level'] == 'Verbose' :
			t0 = tt
			
		# Initialize transaction parameters
		w_id = params["w_id"]
		d_id = params["d_id"]
		threshold = params["threshold"]
		
		# Setup Redis pipelining
		node = self.shard(w_id)
		rdr = self.r_pipes[node]
		wtr = self.w_pipes[node]
		
		#--------------------
		# Get Order ID Query
		#--------------------
		district_key = self.safeKey([w_id, d_id])
		rdr.hget('DISTRICT.' + district_key, 'D_NEXT_O_ID')
		results = rdr.execute()
		o_id = results[0]
		
		if self.debug['stock-level'] == 'Verbose' :
			print 'Get Order ID Query:', time.time() - t0
			t0 = time.time()
			
		#-----------------------
		# Get Stock Count Query
		#-----------------------
		stock_counts = {}
		si_key = self.safeKey([o_id, d_id, w_id])
		rdr.smembers('ORDER_LINE.INDEXES.SUMOLAMOUNT.' + si_key)
		results = rdr.execute()
		line_ids = results[0]
		
		for line_id in line_ids :
			rdr.hgetall('ORDER_LINE.' + str(line_id))
		order_lines = rdr.execute()
		
		for line['OL_I_ID'] in order_lines :
			stock_key = self.safeKey([w_id, line])
			rdr.hget('STOCK.' + stock_key, 'S_QUANTITY')
		stocks = rdr.execute()
		
		for index in range(len(order_lines)) :
			if int(order_lines[index]['OL_I_ID']) < int(o_id) and int(order_lines[index]['OL_I_ID']) > int(o_id) - 20 and float(stocks[index]) < threshold :
				stock_counts[order_lines[index]['OL_I_ID']] = order_lines[index]['OL_I_ID']
		
		if self.debug['stock-level'] == 'Verbose' :
			print 'Get Stock Count Query:', time.time() - t0
		if self.debug['stock-level'] != 'None' :
			print 'TXN STOCK LEVEL:', time.time() - tt
			
		return len(stock_counts)
		
	#------------------------------------------------------------------------
	# Load the specified configuration for Redis TPC-C run
	#
	# @param dictionary config (configuration options)
	#------------------------------------------------------------------------
	def loadConfig(self, config) : 
		for key in RedisDriver.DEFAULT_CONFIG.keys() :
			assert key in config, "Missing parameter '%s' in the %s configuration" % (key, self.name)
		
		hosts = config["databases"].split()	
		first = True
		c_num = 0
		for host in hosts :
			db, port_str = host.split(':')
			port = int(port_str)
			print 'Connectiong to host %s on port %s' % (db, port)
			self.databases.append(redis.Redis(host=db, port=port, db=0))
			print str(self.databases[c_num].ping())
			self.r_pipes.append(self.databases[c_num].pipeline(False))
			self.r_sizes.append(0)
			self.w_pipes.append(self.databases[c_num].pipeline(True))
			self.w_sizes.append(0)
			if (first) :
				first = False
				self.metadata = redis.Redis(host=db, port=port, db=0)
			c_num += 1
			self.db_count += 1
		
		# Reset Databases if required
		if config['reset'] :
			for db in self.databases :
				db.flushall()
				
		# Process Debugging Levels
		self.debug['load'] = config['debug-load']
		self.debug['delivery'] = config['debug-delivery']
		self.debug['new-order'] = config['debug-new-order']
		self.debug['order-status'] = config['debug-order-status']
		self.debug['payment'] = config['debug-payment']
		self.debug['stock-level'] = config['debug-stock-level']
		
		if config['host-info'] != 'None' :
			print 'TPC-C Benchmark Running on Redis with %s nodes' % (len(hosts))
		if config['host-info'] == 'Verbose' :
			for host in hosts :
				db, port = host.split(':')
				print 'Host: %s | Port: %s' % (db, port)
		# End loadConfig()

	#------------------------------------------------------------------------
	# Post-processing function for data loading
	#------------------------------------------------------------------------
	def loadFinish(self) :
		# Check to see if pipelines need to be flushed
		for index in range(len(self.w_pipes)) :
			if self.w_sizes[index] > 0 :
				if self.debug['load'] != 'None' :
					print index, 
				self.w_pipes[index].execute()
				self.w_sizes[index] = 0
		
		elapsed = time.time() - self.t0
		print ''
		print 'Loading Complete: ' + str(elapsed) + ' elapsed'
		
		# Store Metadata
		for table, next in self.next_scores.items() :
			self.metadata.set(table + '.next_score', next)
	# End loadFinish()
	
	#------------------------------------------------------------------------
	# Pre-pocessing function for data loading
	#------------------------------------------------------------------------
	def loadStart(self) :
		if self.debug['load'] != 'None':
			print 'Starting data load'
		self.t0 = time.time()
		self.next_scores = {
			'WAREHOUSE'  : 1,
			'DISTRICT'   : 1,
			'ITEM'       : 1,
			'CUSTOMER'   : 1,
			'HISTORY'    : 1,
			'STOCK'      : 1,
			'ORDERS'     : 1,
			'NEW_ORDER'  : 1,
			'ORDER_LINE' : 1,
		}	
	# End loadStart()
	
	#------------------------------------------------------------------------
	# Load tuples into a table for TPC-C benchmarking
	#
	# @param string table name
	# @param list of tuples corresponding to table schema
	#------------------------------------------------------------------------
	def loadTuples(self, tableName, tuples) :
		
		# Instantiate Column-mapping
		column_map = self.TABLES[tableName]['map']
		if self.debug['load'] == 'Verbose' :
			print tableName,
			
		for record in tuples :
			# Determine at which node to store this data
			node = 'ALL'
			if tableName == 'WAREHOUSE' :
				node = self.shard(record[column_map['W_ID']])
				key = self.safeKey([record[0]])
				self.w_pipes[node].sadd('WAREHOUSE.IDS', key)
				self.w_pipes[node].hmset(
					'WAREHOUSE.' + key, 
					{
						'W_ID' : record[0],
						'W_NAME' : record[1],
						'W_STREET_1' : record[2],
						'W_STREET_2' : record[3],
						'W_CITY'     : record[4],
						'W_STATE'    : record[5],
						'W_ZIP'      : record[6],
						'W_TAX'      : record[7],
						'W_YTD'      : record[8],
					}
				)
				self.w_sizes[node] += 2	
			elif tableName == 'DISTRICT' :
				node = self.shard(record[column_map['D_W_ID']])
				key = self.safeKey([record[1], record[0]])
				self.w_pipes[node].sadd('DISTRICT.IDS', key)
				self.w_pipes[node].hmset(
					'DISTRICT.' + key,
					{
						'D_ID'        : record[0],
						'D_W_ID'      : record[1],
						'D_NAME'      : record[2],
						'D_STREET_1'  : record[3],
						'D_STREET_2'  : record[4],
						'D_CITY'      : record[5],
						'D_STATE'     : record[6],
						'D_ZIP'       : record[7],
						'D_TAX'       : record[8],
						'D_YTD'       : record[9],
						'D_NEXT_O_ID' : record[10],
					}
				)
				self.w_sizes[node] += 2
			elif tableName == 'CUSTOMER' :
				node = self.shard(record[column_map['C_W_ID']])
				key = self.safeKey([record[2], record[1], record[0]])
				self.w_pipes[node].sadd('CUSTOMER.IDS', key)
				self.w_pipes[node].hmset(
					'CUSTOMER.' + key,
					{
						'C_ID'           : record[0],
						'C_D_ID'         : record[1],
						'C_W_ID'         : record[2],
						'C_FIRST'        : record[3],
						'C_MIDDLE'       : record[4],
						'C_LAST'         : record[5],
						'C_STREET_1'     : record[6],
						'C_STREET_2'     : record[7],
						'C_CITY'         : record[8],
						'C_ZIP'          : record[9],
						'C_PHONE'        : record[10],
						'C_SINCE'        : record[11],
						'C_CREDIT'       : record[12],
						'C_CREDIT_LIM'   : record[13],
						'C_DISCOUNT'     : record[14],
						'C_BALANCE'      : record[15],
						'C_YTD_PAYMENT'  : record[16],
						'C_PAYMENT_CNT'  : record[17],
						'C_DELIVERY_CNT' : record[18],
						'C_DATA'         : record[19],
					}
				)
				
				# Add Special Index for Customer Table
				index_key = self.safeKey([record[2], record[1], record[5]])
				self.w_pipes[node].sadd(
					'CUSTOMER.INDEXES.NAMESEARCH.' + index_key,
					key
				)
				self.w_sizes[node] += 3
			elif tableName == 'HISTORY' : 
				node = self.shard(record[column_map['H_W_ID']])
				key = self.safeKey([self.next_scores['HISTORY']])
				self.w_pipes[node].sadd('HISTORY.IDS', key)
				self.w_pipes[node].hmset(
					'HISTORY.' + key,
					{
						'H_C_ID'   : record[0],
						'H_C_D_ID' : record[1],
						'H_C_W_ID' : record[2],
						'H_D_ID'   : record[3],
						'H_W_ID'   : record[4],
						'H_DATE'   : record[5],
						'H_AMOUNT' : record[6],
						'H_DATA'   : record[7],
					}
				)
				self.w_sizes[node] += 2
			elif tableName == 'STOCK' :
				node = self.shard(record[column_map['S_W_ID']])
				key = self.safeKey([record[1], record[0]])
				self.w_pipes[node].sadd('STOCK.IDS', key)
				self.w_pipes[node].hmset(
					'STOCK.' + key,
					{
						'S_I_ID'       : record[0],
						'S_W_ID'       : record[1],
						'S_QUANTITY'   : record[2],
						'S_DIST_01'    : record[3],
						'S_DIST_02'    : record[4],
						'S_DIST_03'    : record[5],
						'S_DIST_04'    : record[6],
						'S_DIST_05'    : record[7],
						'S_DIST_06'    : record[8],
						'S_DIST_07'    : record[9],
						'S_DIST_08'    : record[10],
						'S_DIST_09'    : record[11],
						'S_DIST_10'    : record[12],
						'S_YTD'        : record[13],
						'S_ORDER_CNT'  : record[14],
						'S_REMOTE_CNT' : record[15],
						'S_DATA'       : record[16],
						
					}
				)
				self.w_sizes[node] += 2
			elif tableName == 'ORDERS' :
				node = self.shard(record[column_map['O_W_ID']])
				key = self.safeKey([
					record[0], 
					record[3], 
					record[1], 
					record[2]
				])
				self.w_pipes[node].sadd('ORDER.IDS', key)
				self.w_pipes[node].hmset(
					'ORDER.' + key,
					{
						'O_ID'         : record[0],
						'O_D_ID'       : record[1],
						'O_W_ID'       : record[2],
						'O_C_ID'       : record[3],
						'O_ENTRY_D'    : record[4],
						'O_CARRIER_ID' : record[5],
						'O_OL_CNT'     : record[6],
						'O_ALL_LOCAL'  : record[7],
					}
				)
				
				# Add Special Index for Order Table
				index_key = self.safeKey([record[2], record[1], record[3]])
				self.w_pipes[node].sadd(
					'ORDERS.INDEXES.ORDERSEARCH.' + index_key,
					key
				)	
				self.w_sizes[node] += 3
			elif tableName == 'NEW_ORDER' :
				node = self.shard(record[column_map['NO_W_ID']])
				key = self.safeKey([record[1], record[2], record[0]])
				self.w_pipes[node].sadd('NEW_ORDER.IDS', key)
				self.w_pipes[node].hmset(
					'NEW_ORDER.' + key,
					{
						'NO_O_ID' : record[0],
						'NO_D_ID' : record[1],
						'NO_W_ID' : record[2],
					}
				)
				
				# Add Special Index for New Order Table
				index_key = self.safeKey([record[1], record[2]])
				self.w_pipes[node].sadd(
					'NEW_ORDER.INDEXES.GETNEWORDER.' + index_key,
					key
				)
				self.w_sizes[node] += 3
			elif tableName == 'ORDER_LINE' :
				node = self.shard(record[column_map['OL_W_ID']])
				key = self.safeKey([
					record[2],
					record[1],
					record[0],
					record[3]
				])
				self.w_pipes[node].sadd('ORDER_LINE.IDS', key)
				self.w_pipes[node].hmset(
					'ORDER_LINE.' + key,
					{
						'OL_O_ID'        : record[0],
						'OL_D_ID'        : record[1],
						'OL_W_ID'        : record[2],
						'OL_NUMBER'      : record[3],
						'OL_I_ID'        : record[4],
						'OL_SUPPLY_W_ID' : record[5],
						'OL_DELIVERY_D'  : record[6],
						'OL_QUANTITY'    : record[7],
						'OL_AMOUNT'      : record[8],
						'OL_DIST_INFO'   : record[9],
					}
				)
				
				# Add Special Index for Order Line Table
				index_key = self.safeKey([record[0], record[1], record[2]])
				self.w_pipes[node].sadd(
					'ORDER_LINE.INDEXES.SUMOLAMOUNT.' + index_key,
					key
				)
				self.w_sizes[node] += 3
			elif tableName == 'ITEMS' :
				key = self.safeKey([record[0]]);
				pi = 0
				for pipe in self.w_pipes :
					pipe.sadd('ITEM.IDS', key)
					pipe.hmset(
						'ITEM.' + key,
						{
							'I_ID'    : record[0],
							'I_IM_ID' : record[1],
							'I_NAME'  : record[2],
							'I_PRICE' : record[3],
							'I_DATA'  : record[4],
						}
					)
					self.w_sizes[pi] += 2
				pass
			self.next_scores[tableName] += 1
			
			#print key,
			# Check to see if pipelines need to be flushed
			for index in range(len(self.w_pipes)) :
				if self.w_sizes[index] > 10000 :
					if self.debug['load'] != 'None' :
						print index, 
						sys.stdout.flush()
					self.w_pipes[index].execute()
					self.w_sizes[index] = 0
		
		if self.debug['load'] == 'Verbose' :
			print ''
	# End loadTuples()
	
	#------------------------------------------------------------------------
	# Return default configuration when none is specified via command line
	#
	# @return dictionary configuration parameters
	#------------------------------------------------------------------------
	def makeDefaultConfig(self) : 
		return self.DEFAULT_CONFIG
	# End makeDefaultConfig()
	
	#------------------------------------------------------------------------
	# Create a safe key for Redis by removing invalid characters from 
	# input list
	#
	# @param list keys
	#------------------------------------------------------------------------
	def safeKey(self, keys) :
		new_keys = []
		for k in keys :
			new_keys.append(str(k))
		return self.KEY_SEPARATOR.join(new_keys
			).replace('\n', '').replace(' ','')
	# End safeKey()
	
	#------------------------------------------------------------------------
	# Sharding Method for determine which not to access
	#
	# @param string w_id warehouse id
	# @return int
	#------------------------------------------------------------------------
	def shard(self, w_id) :
		return int(w_id) % self.db_count
	# End shard()