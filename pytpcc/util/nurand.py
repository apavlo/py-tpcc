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

import rand

def makeForLoad():
    """Create random NURand constants, appropriate for loading the database."""
    cLast = rand.number(0, 255)
    cId = rand.number(0, 1023)
    orderLineItemId = rand.number(0, 8191)
    return NURandC(cLast, cId, orderLineItemId)

def validCRun(cRun, cLoad):
    """Returns true if the cRun value is valid for running. See TPC-C 2.1.6.1 (page 20)"""
    cDelta = abs(cRun - cLoad)
    return 65 <= cDelta and cDelta <= 119 and cDelta != 96 and cDelta != 112

def makeForRun(loadC):
    """Create random NURand constants for running TPC-C. TPC-C 2.1.6.1. (page 20) specifies the valid range for these constants."""
    cRun = rand.number(0, 255)
    while validCRun(cRun, loadC.cLast) == False:
        cRun = rand.number(0, 255)
    assert validCRun(cRun, loadC.cLast)
    
    cId = rand.number(0, 1023)
    orderLineItemId = rand.number(0, 8191)
    return NURandC(cRun, cId, orderLineItemId)

class NURandC:
    def __init__(self, cLast, cId, orderLineItemId):
        self.cLast = cLast
        self.cId = cId
        self.orderLineItemId = orderLineItemId
