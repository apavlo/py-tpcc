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

import random

SYLLABLES = [ "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" ]

nurand = None # NURand
def setNURand(nu):
    global nurand
    nurand = nu
## DEF

def NURand(a, x, y):
    """A non-uniform random number, as defined by TPC-C 2.1.6. (page 20)."""
    global nurand
    assert x <= y
    assert nurand != None
    
    if a == 255:
        c = nurand.cLast
    elif a == 1023:
        c = nurand.cId
    elif a == 8191:
        c = nurand.orderLineItemId
    else:
        raise Exception("a = " + a + " is not a supported value")
    
    return (((number(0, a) | number(x, y)) + c) % (y - x + 1)) + x
## DEF

def number(minimum, maximum):
    value = random.randint(minimum, maximum)
    assert minimum <= value and value <= maximum
    return value
## DEF

def numberExcluding(minimum, maximum, excluding):
    """An in the range [minimum, maximum], excluding excluding."""
    assert minimum < maximum
    assert minimum <= excluding and excluding <= maximum

    ## Generate 1 less number than the range
    num = number(minimum, maximum-1)

    ## Adjust the numbers to remove excluding
    if num >= excluding: num += 1
    assert minimum <= num and num <= maximum and num != excluding
    return num
## DEF 

def fixedPoint(decimal_places, minimum, maximum):
    assert decimal_places > 0
    assert minimum < maximum

    multiplier = 1
    for i in range(0, decimal_places):
        multiplier *= 10

    int_min = int(minimum * multiplier + 0.5)
    int_max = int(maximum * multiplier + 0.5)

    return float(number(int_min, int_max) / float(multiplier))
## DEF

def selectUniqueIds(numUnique, minimum, maximum):
    rows = set()
    for i in range(0, numUnique):
        index = None
        while index == None or index in rows:
            index = number(minimum, maximum)
        ## WHILE
        rows.add(index)
    ## FOR
    assert len(rows) == numUnique
    return rows
## DEF

def astring(minimum_length, maximum_length):
    """A random alphabetic string with length in range [minimum_length, maximum_length]."""
    return randomString(minimum_length, maximum_length, 'a', 26)
## DEF

def nstring(minimum_length, maximum_length):
    """A random numeric string with length in range [minimum_length, maximum_length]."""
    return randomString(minimum_length, maximum_length, '0', 10)
## DEF

def randomString(minimum_length, maximum_length, base, numCharacters):
    length = number(minimum_length, maximum_length)
    baseByte = ord(base)
    string = ""
    for i in range(length):
        string += chr(baseByte + number(0, numCharacters-1))
    return string
## DEF

def makeLastName(number):
    """A last name as defined by TPC-C 4.3.2.3. Not actually random."""
    global SYLLABLES
    assert 0 <= number and number <= 999
    indicies = [ number/100, (number/10)%10, number%10 ]
    return "".join(map(lambda x: SYLLABLES[x], indicies))
## DEF

def makeRandomLastName(maxCID):
    """A non-uniform random last name, as defined by TPC-C 4.3.2.3. The name will be limited to maxCID."""
    min_cid = 999
    if (maxCID - 1) < min_cid: min_cid = maxCID - 1
    return makeLastName(NURand(255, 0, min_cid))
## DEF
