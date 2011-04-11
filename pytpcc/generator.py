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
    w_tax = makeTax()
    w_ytd = constants.INITIAL_W_YTD
    w_address = addAddress()
    return [w_tx, w_ytd] + w_addresss
## DEF

def generateDistrict(d_w_id, d_id):
    d_tax = makeTax()
    d_ytd = constants.INITIAL_D_YTD
    d_next_o_id = m_parameters.customersPerDistrict + 1
    d_address = addAddress()
    return [d_tax, d_ytd, d_next_o_id] + d_address
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
    c_zip = makeZip()

    return [ c_id, c_d_id, c_w_id, c_first, c_middle, c_last, \
             c_street1, c_street2, c_city, c_state, c_zip, \
             c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, \
             c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data ]
## DEF

def addAddress():
    """
        Returns a name and a street address 
        Used by both generateWarehouse and generateDistrict.
    """
    name = rand.astring(constants.MIN_NAME, constants.MAX_NAME)
    return [ name ] + addStreetAddress()
## DEF

def addStreetAddress():
    """
        Returns a list for a street address
        Used for warehouses, districts and customers.
    """
    street1 = m_rand.astring(constants.MIN_STREET, constants.MAX_STREET)
    street2 = m_rand.astring(constants.MIN_STREET, constants.MAX_STREET)
    city = m_rand.astring(constants.MIN_CITY, constants.MAX_CITY)
    state = m_rand.astring(constants.STATE, constants.STATE)
    zip = makeZip()

    return [ street1, street2, city, state, zip ]
## DEF

def selectUniqueIds(numUnique, minimum, maximum):
    rows = set()
    for i in range(0, numUnique):
        index = None
        while index == None or index in rows:
            index = rand.number(minimum, maximum)
        ## WHILE
        rows.add(index)
    ## FOR
    assert len(rows) == numUnique
    return rows
## DEF

def makeTax():
    return rand.fixedPoint(constants.TAX_DECIMALS, constants.MIN_TAX, constants.MAX_TAX)
## DEF

def makeZip():
    length = constants.ZIP_LENGTH - len(constants.ZIP_SUFFIX)
    return m_rand.nstring(length, length) + constants.ZIP_SUFFIX
## DEF

def fillOriginal(data):
    """
        a string with ORIGINAL_STRING at a random position
    """
    originalLength = len(constants.ORIGINAL_STRING)
    position = m_rand.number(0, len(data) - originalLength)
    out = data[:position] + constants.ORIGINAL_STRING + data[position + originalLength:]
    assert len(out) == len(data)
    return out
## DEF