+ ----------------------------------------------- +
+ Python TPC-C                                    +
+ ----------------------------------------------- +


The basic idea is that you will need to create a new driver file that 
implements the functions defined in "abstractdriver.py". One function will 
load in the tuples into your database for a given table. Then there are five 
separate functions that execute the given transaction based on a set of input 
parameters. All the work for generating the tuples and the input parameters 
for the transactions has been done for you.

Here's what you need to do to get started:

(1) Download the source code from Github:

https://github.com/apavlo/py-tpcc/tree/master/pytpcc

(2) Create a new file in the 'drivers' directory for your system that follows 
the proper naming convention. For example, if your system is 'MongoDB', then 
your new file will be called 'mongodbdriver.py' and that file will contain a 
new class called 'MongodbDriver' (note the capitalization).

(3) Inside your class you will need to implement the required functions of 
defined in AbstractDriver. There is documentation on what these need to do 
also available on Github:

https://github.com/apavlo/py-tpcc/wiki

(3) Try running your system. I would start by defining the configuration file 
that gets returned with by the 'makeDefaultConfig' function in your driver and 
then implement the data loading part first, since that will guide how you 
actually execute the transactions. Using 'MongoDB' as an example again, you 
can print out the driver's configuration dict to a file:

$ python ./tpcc.py --print-config mongodb > mongodb.config

Make any changes you need to 'mongodb.config' (e.g., passwords, hostnames). 
Then test the loader:

$ python ./tpcc.py --no-execute --config=mongodb.config mongodb

You can use the CSV driver if you want to see what the data or transaction 
input parameters will look like. The following command will dump out just the 
input to the driver's functions to files in /tmp/tpcc-*

$ python ./tpcc.py csv

You can also look at my SqliteDriver implementation to get an idea of what 
your transaction implementation functions need to do:

https://github.com/apavlo/py-tpcc/blob/master/pytpcc/drivers/sqlitedriver.py
