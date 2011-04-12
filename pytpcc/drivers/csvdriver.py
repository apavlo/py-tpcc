#!/usr/bin/env python
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

import os
import csv
from abstractdriver import *

## ==============================================
## CSVDriver
## ==============================================
class CsvDriver(AbstractDriver):
    
    def __init__(self):
        super(CsvDriver, self).__init__("csv")
        self.directory = None
        self.outputs = { }
        
    ## DEF
    
    def loadConfig(self, config):
        self.directory = config["directory"]
        assert os.path.exists(self.directory), "Invalid directory '%s'" % self.directory
    ## DEF
    
    def loadTuples(self, table, tuples):
        if not table in self.outputs:
            path = os.path.join(self.directory, "%s.csv" % table)
            self.outputs[table] = csv.writer(open(path, 'wb'), quoting=csv.QUOTE_ALL)
        ## IF
        self.outputs[table].writerows(tuples)
    ## DEF
## CLASS

        
