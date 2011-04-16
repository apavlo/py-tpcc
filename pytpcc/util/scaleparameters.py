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

import constants

def makeDefault(warehouses):
    return ScaleParameters(constants.NUM_ITEMS, \
                           warehouses, \
                           constants.DISTRICTS_PER_WAREHOUSE, \
                           constants.CUSTOMERS_PER_DISTRICT, \
                           constants.INITIAL_NEW_ORDERS_PER_DISTRICT)
## DEF

def makeWithScaleFactor(warehouses, scaleFactor):
    assert scaleFactor >= 1.0

    items = int(constants.NUM_ITEMS/scaleFactor)
    if items <= 0: items = 1
    districts = int(max(constants.DISTRICTS_PER_WAREHOUSE, 1))
    customers = int(max(constants.CUSTOMERS_PER_DISTRICT/scaleFactor, 1))
    newOrders = int(max(constants.INITIAL_NEW_ORDERS_PER_DISTRICT/scaleFactor, 0))

    return ScaleParameters(items, warehouses, districts, customers, newOrders)
## DEF

class ScaleParameters:
    
    def __init__(self, items, warehouses, districtsPerWarehouse, customersPerDistrict, newOrdersPerDistrict):
        assert 1 <= items and items <= constants.NUM_ITEMS
        self.items = items
        assert warehouses > 0
        self.warehouses = warehouses
        self.starting_warehouse = 1
        assert 1 <= districtsPerWarehouse and districtsPerWarehouse <= constants.DISTRICTS_PER_WAREHOUSE
        self.districtsPerWarehouse = districtsPerWarehouse
        assert 1 <= customersPerDistrict and customersPerDistrict <= constants.CUSTOMERS_PER_DISTRICT
        self.customersPerDistrict = customersPerDistrict
        assert 0 <= newOrdersPerDistrict and newOrdersPerDistrict <= constants.CUSTOMERS_PER_DISTRICT
        assert newOrdersPerDistrict <= constants.INITIAL_NEW_ORDERS_PER_DISTRICT
        self.newOrdersPerDistrict = newOrdersPerDistrict
        self.ending_warehouse = (self.warehouses + self.starting_warehouse - 1)
    ## DEF

    def __str__(self):
        out =  "%d items\n" % self.items
        out += "%d warehouses\n" % self.warehouses
        out += "%d districts/warehouse\n" % self.districtsPerWarehouse
        out += "%d customers/district\n" % self.customersPerDistrict
        out += "%d initial new orders/district" % self.newOrdersPerDistrict
        return out
    ## DEF

## CLASS