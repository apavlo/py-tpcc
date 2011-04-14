# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
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

from datetime import datetime

class Results:
    
    def __init__(name):
        self.name = name
        self.data = { }
        self.start = None
        self.stop = None
        
    def start():
        """Mark the benchmark as having been started"""
        assert self.start == None
        self.start = datetime.now()
        return self.start
        
    def stop():
        """Mark the benchmark as having been stopped"""
        assert self.start != None
        assert self.stop == None
        self.stop = datetime.now()
        
    def completed(txn):
        """Record that the benchmark completed an invocation of the given transaction"""
        cnt = self.data.get(txn, 0)
        self.data[txn] = cnt + 1
    
    def __str__():
        if self.start == None:
            raise Exception("Benchmark not started")
        if self.stop == None:
            duration = (datetime.now() - self.start).seconds
        else:
            duration = (self.stop - self.start).seconds
        
        ret = "%s Results [duration=%d seconds]\n%s" % (self.name, duration, "-"*100)
        ret += "\n%22s%-20s%-20s" % ("Transaction", "Total", "Rate")
        for txn in sorted(self.data.keys()):
            rate = "%.02f" % (self.data[txn] / duration)
            ret += "\n  %-20s%-20s%-20s" % (txn, str(self.data[txn]), rate)
        return (ret)
## CLASS