# -*- coding: utf-8 -*-

class ScaleParameters:
    
    STARTING_WAREHOUSE = 1
    
    def __init__(items, warehouses, districtsPerWarehouse, customersPerDistrict, newOrdersPerDistrict):
        assert 1 <= items and items <= constants.NUM_ITEMS
        self.items = items
        assert warehouses > 0
        self.warehouses = warehouses
        self.starting_warehouse = STARTING_WAREHOUSE
        assert 1 <= districtsPerWarehouse and districtsPerWarehouse <= constants.DISTRICTS_PER_WAREHOUSE
        self.districtsPerWarehouse = districtsPerWarehouse
        assert 1 <= customersPerDistrict and customersPerDistrict <= constants.CUSTOMERS_PER_DISTRICT
        self.customersPerDistrict = customersPerDistrict
        assert 0 <= newOrdersPerDistrict and newOrdersPerDistrict <= constants.CUSTOMERS_PER_DISTRICT
        assert newOrdersPerDistrict <= constants.INITIAL_NEW_ORDERS_PER_DISTRICT
        self.newOrdersPerDistrict = newOrdersPerDistrict
        self.max_w_id = (self.warehouses + self.starting_warehouse - 1)
    ## DEF

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
        districts = max(constants.DISTRICTS_PER_WAREHOUSE, 1)
        customers = max(constants.CUSTOMERS_PER_DISTRICT/scaleFactor, 1)
        newOrders = max(constants.INITIAL_NEW_ORDERS_PER_DISTRICT/scaleFactor, 0)

        return ScaleParameters(items, warehouses, districts, customers, newOrders)
    ## DEF

    def __str__():
        out = items + " items\n"
        out += warehouses + " warehouses\n"
        out += districtsPerWarehouse + " districts/warehouse\n"
        out += customersPerDistrict + " customers/district\n"
        out += newOrdersPerDistrict + " initial new orders/district"
        return out
    ## DEF

## CLASS