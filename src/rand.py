# -*- coding: utf-8 -*-

import random

#class NURandC:
    #def __init__(cLast, cId, orderLineItemId):
        #self.cLast = cLast
        #self.cId = cId
        #self.orderLineItemId = orderLineItemId
    ### DEF

        #/** Create random NURand constants, appropriate for loading the database. */
        #public static NURandC makeForLoad(RandomGenerator generator) {
            #return new NURandC(generator.number(0, 255), generator.number(0, 1023),
                    #generator.number(0, 8191))
        #}

        #/** @returns true if the cRun value is valid for running. See TPC-C 2.1.6.1 (page 20). */
        #private static boolean validCRun(cRun, cLoad) {
            #cDelta = Math.abs(cRun - cLoad)
            #return 65 <= cDelta && cDelta <= 119 && cDelta != 96 && cDelta != 112
        #}

        #/** Create random NURand constants for running TPC-C. TPC-C 2.1.6.1. (page 20) specifies the
        #valid range for these constants. */
        #public static NURandC makeForRun(RandomGenerator generator, NURandC loadC) {
            #cRun = generator.number(0, 255)
            #while (!validCRun(cRun, loadC.cLast)) {
                #cRun = generator.number(0, 255)
            #}
            #assert validCRun(cRun, loadC.cLast)

            #return new NURandC(cRun, generator.number(0, 1023), generator.number(0, 8191))
        #}

    #}


def number(minimum, maximum):
    assert minimum <= maximum
    range_size = maximum - minimum + 1
    value = random.nextInt(range_size)
    value += minimum
    assert minimum <= value and value <= maximum
    return value
## DEF

def numberExcluding(minimum, maximum, excluding):
    """
        An in the range [minimum, maximum], excluding excluding.
    """
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

def astring(minimum_length, maximum_length):
    """
        a random alphabetic string with length in range [minimum_length, maximum_length].
    """
    return randomString(minimum_length, maximum_length, 'a', 26)
## DEF

def nstring(minimum_length, maximum_length):
    """
        a random numeric string with length in range [minimum_length, maximum_length].
    """
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
    """
        a last name as defined by TPC-C 4.3.2.3. Not actually random.
    """
    SYLLABLES [ "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" ]

    assert 0 <= number and number <= 999
    indicies = [ number/100, (number/10)%10, number%10 ]
    return "".join(map(lambda x: SYLLABLES[x], indicies))
## DEF

def makeRandomLastName(maxCID):
    """
        a non-uniform random last name, as defined by TPC-C 4.3.2.3. The name will be
        limited to maxCID.
    """
    min_cid = 999
    if (maxCID - 1) < min_cid: min_cid = maxCID - 1
    return makeLastName(NURand(255, 0, min_cid))
## DEF

def NURand(A, x, y):
    """
        a non-uniform random number, as defined by TPC-C 2.1.6. (page 20).
    """
    assert x <= y
    
    if A == 255:
        C = cValues.cLast
    elif A == 1023:
        C = cValues.cId
    elif A == 8191:
        C = cValues.orderLineItemId
    else:
        raise Exception("A = " + A + " is not a supported value")
    
    return (((number(0, A) | number(x, y)) + C) % (y - x + 1)) + x
## DEF