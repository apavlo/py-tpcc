#!/usr/bin/env python
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

import os
import sys
from datetime import datetime

from util import *
import constants

def generateItem(id, original):
    i_id = id
    i_im_id = rand.number(constants.MIN_IM, constants.MAX_IM)
    i_name = rand.astring(constants.MIN_I_NAME, constants.MAX_I_NAME)
    i_price = rand.fixedPoint(constants.MONEY_DECIMALS, constants.MIN_PRICE, constants.MAX_PRICE)
    i_data = rand.astring(constants.MIN_I_DATA, constants.MAX_I_DATA)
    if original: i_data = fillOriginal(i_data)

    return [i_id, i_im_id, i_name, i_price, i_data]
## DEF

def generateWarehouse(w_id):
    w_tax = generateTax()
    w_ytd = constants.INITIAL_W_YTD
    w_address = generateAddress()
    return [w_id, w_tax, w_ytd] + w_address
## DEF

def generateDistrict(d_w_id, d_id, d_next_o_id):
    d_tax = generateTax()
    d_ytd = constants.INITIAL_D_YTD
    d_address = generateAddress()
    return [d_id, d_w_id, d_tax, d_ytd, d_next_o_id] + d_address
## DEF

def generateCustomer(c_w_id, c_d_id, c_id, badCredit, doesReplicateName):
    c_first = rand.astring(constants.MIN_FIRST, constants.MAX_FIRST)
    c_middle = constants.MIDDLE

    assert 1 <= c_id and c_id <= constants.CUSTOMERS_PER_DISTRICT
    if c_id <= 1000:
        c_last = rand.makeLastName(c_id - 1)
    else:
        c_last = rand.makeRandomLastName(m_parameters.customersPerDistrict)

    c_phone = rand.nstring(constants.PHONE, constants.PHONE)
    c_since = datetime.now()
    c_credit = constants.BAD_CREDIT if badCredit else constants.GOOD_CREDIT
    c_credit_lim = constants.INITIAL_CREDIT_LIM
    c_discount = rand.fixedPoint(constants.DISCOUNT_DECIMALS, constants.MIN_DISCOUNT, constants.MAX_DISCOUNT)
    c_balance = constants.INITIAL_BALANCE
    c_ytd_payment = constants.INITIAL_YTD_PAYMENT
    c_payment_cnt = constants.INITIAL_PAYMENT_CNT
    c_delivery_cnt = constants.INITIAL_DELIVERY_CNT
    c_data = rand.astring(constants.MIN_C_DATA, constants.MAX_C_DATA)

    c_street1 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
    c_street2 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
    c_city = rand.astring(constants.MIN_CITY, constants.MAX_CITY)
    c_state = rand.astring(constants.STATE, constants.STATE)
    c_zip = generateZip()

    return [ c_id, c_d_id, c_w_id, c_first, c_middle, c_last, \
             c_street1, c_street2, c_city, c_state, c_zip, \
             c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, \
             c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data ]
## DEF

def generateOrder(o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt, newOrder):
    """Returns the generated o_ol_cnt value."""
    o_entry_d = datetime.now()
    o_carrier_id = constants.NULL_CARRIER_ID if newOrder else rand.number(constants.MIN_CARRIER_ID, constants.MAX_CARRIER_ID)
    o_all_local = constants.INITIAL_ALL_LOCAL
    return [ o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local ]
## DEF

def generateOrderLine(ol_w_id, ol_d_id, ol_o_id, ol_number, max_items, newOrder):
    ol_i_id = rand.number(1, max_items)
    ol_supply_w_id = ol_w_id
    ol_delivery_d = datetime.now()
    ol_quantity = constants.INITIAL_QUANTITY

    if newOrder == False:
        ol_amount = 0.00
    else:
        ol_amount = rand.fixedPoint(constants.MONEY_DECIMALS, constants.MIN_AMOUNT, constants.MAX_PRICE * constants.MAX_OL_QUANTITY)
        ol_delivery_d = None
    ol_dist_info = rand.astring(constants.DIST, constants.DIST)

    return [ ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info ]
## DEF

def generateStock(s_w_id, s_i_id, original):
    s_quantity = rand.number(constants.MIN_QUANTITY, constants.MAX_QUANTITY);
    s_ytd = 0;
    s_order_cnt = 0;
    s_remote_cnt = 0;

    s_data = rand.astring(constants.MIN_I_DATA, constants.MAX_I_DATA);
    if original: fillOriginal(s_data)

    s_dists = [ ]
    for i in range(0, constants.DISTRICTS_PER_WAREHOUSE):
        s_dists.append(rand.astring(constants.DIST, constants.DIST))
    
    return [ s_i_id, s_w_id, s_quantity ] + \
           s_dists + \
           [ s_ytd, s_order_cnt, s_remote_cnt, s_data ]
## DEF

def generateHistory(h_c_w_id, h_c_d_id, h_c_id):
    h_w_id = h_c_w_id
    h_d_id = h_c_d_id
    h_date = datetime.now()
    h_amount = constants.INITIAL_AMOUNT
    h_data = rand.astring(constants.MIN_DATA, constants.MAX_DATA)
    return [ h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data ]
## DEF

def generateAddress():
    """
        Returns a name and a street address 
        Used by both generateWarehouse and generateDistrict.
    """
    name = rand.astring(constants.MIN_NAME, constants.MAX_NAME)
    return [ name ] + generateStreetAddress()
## DEF

def generateStreetAddress():
    """
        Returns a list for a street address
        Used for warehouses, districts and customers.
    """
    street1 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
    street2 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
    city = rand.astring(constants.MIN_CITY, constants.MAX_CITY)
    state = rand.astring(constants.STATE, constants.STATE)
    zip = generateZip()

    return [ street1, street2, city, state, zip ]
## DEF

def generateTax():
    return rand.fixedPoint(constants.TAX_DECIMALS, constants.MIN_TAX, constants.MAX_TAX)
## DEF

def generateZip():
    length = constants.ZIP_LENGTH - len(constants.ZIP_SUFFIX)
    return rand.nstring(length, length) + constants.ZIP_SUFFIX
## DEF

def fillOriginal(data):
    """
        a string with ORIGINAL_STRING at a random position
    """
    originalLength = len(constants.ORIGINAL_STRING)
    position = rand.number(0, len(data) - originalLength)
    out = data[:position] + constants.ORIGINAL_STRING + data[position + originalLength:]
    assert len(out) == len(data)
    return out
## DEF