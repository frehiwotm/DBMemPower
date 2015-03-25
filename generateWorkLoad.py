#!/usr/bin/env python3

import sqlite3
import csv
import time



start_time = time.clock()

conn = sqlite3.connect ('testdb.db')
c = conn.cursor()

'''
with open('tpch-create.sql') as mytables:
 schema = mytables.read()
 mytables.close()

#all SQL commands (split on ';')
sqlCommands = schema.split(';')

for command in sqlCommands:
 try:
  c.execute(command)
 except sqlite3.OperationalError as e:
  print("command skipped:", e)

conn.commit()
conn.close()

#elapsed_time = (time.clock()) - (start_time())
print("The tables are created")
#print("Time elapsed: {} seconds".format(elapsed_time))
'''
with open('csvfiles.txt','r') as f:
 content = f.readlines()

for cont in content:
 cont = cont.strip('\n');
 if cont == 'nation.csv':
    pass
    ''''	
    with open('nation.csv','r') as csvReader:
        nationreader = csv.reader(csvReader, delimiter= '|')
        for row in nationreader:
        to_db = [bytes(row[0], "utf-8"), bytes(row[1], "utf-8"), bytes(row[2], "utf-8"), bytes(row[3],"utf-8")]
        c.execute("INSERT INTO NATION (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) VALUES (?, ?, ?, ?);", 			  to_db)
        '''
 elif cont == 'lineitem.csv':
    with open('lineitem.csv','r') as csvReader:
        lineitemreader = csv.reader(csvReader, delimiter= '|')
        for row in lineitemreader:
         to_db = [bytes(row[0], "utf-8"), bytes(row[1], "utf-8"), bytes(row[2], "utf-8"), bytes(row[3],"utf-8"), bytes(row[4], "utf-8"), bytes(row[5], "utf-8"), bytes(row[6], "utf-8"), bytes(row[7], "utf-8"), bytes(row[8], "utf-8"), bytes(row[9], "utf-8"), bytes(row[10], "utf-8"), bytes(row[11], "utf-8"), bytes(row[12], "utf-8"), bytes(row[13], "utf-8"), bytes(row[14], "utf-8"), bytes(row[15], "utf-8")]
         c.execute("INSERT INTO LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", 			  to_db)	
 elif cont == 'customer.csv':
    with open('customer.csv','r') as csvReader:
        reader = csv.reader(csvReader, delimiter= '|')
        for row in reader:
         to_db = [bytes(row[0], "utf-8"), bytes(row[1], "utf-8"), bytes(row[2], "utf-8"), bytes(row[3],"utf-8"), bytes(row[4], "utf-8"), bytes(row[5], "utf-8"), bytes(row[6], "utf-8"), bytes(row[7], "utf-8")]
         c.execute("INSERT INTO CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",to_db)
 elif cont == 'orders.csv':
    with open('orders.csv','r') as csvReader:
        reader = csv.reader(csvReader, delimiter= '|')
        for row in reader:
         to_db = [bytes(row[0], "utf-8"), bytes(row[1], "utf-8"), bytes(row[2], "utf-8"), bytes(row[3],"utf-8"), bytes(row[4], "utf-8"), bytes(row[5], "utf-8"), bytes(row[6], "utf-8"), bytes(row[7], "utf-8"), bytes(row[8], "utf-8")]
         c.execute("INSERT INTO ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",to_db)
 elif cont == 'part.csv':
     with open('part.csv','r') as csvReader:
        reader = csv.reader(csvReader, delimiter= '|')
        for row in reader:
         to_db = [bytes(row[0], "utf-8"), bytes(row[1], "utf-8"), bytes(row[2], "utf-8"), bytes(row[3],"utf-8"), bytes(row[4], "utf-8"), bytes(row[5], "utf-8"), bytes(row[6], "utf-8"), bytes(row[7], "utf-8"), bytes(row[8], "utf-8")]
         c.execute("INSERT INTO PART (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",to_db)
 elif cont == 'partsupp.csv':
    with open('partsupp.csv','r') as csvReader:
        reader = csv.reader(csvReader, delimiter= '|')
        for row in reader:
         to_db = [bytes(row[0], "utf-8"), bytes(row[1], "utf-8"), bytes(row[2], "utf-8"), bytes(row[3],"utf-8"), bytes(row[4], "utf-8")]
         c.execute("INSERT INTO PARTSUPP (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) VALUES (?, ?, ?, ?, ?);",to_db)
 elif cont == 'region.csv':
    with open('region.csv','r') as csvReader:
        reader = csv.reader(csvReader, delimiter= '|')
        for row in reader:
         to_db = [bytes(row[0], "utf-8"), bytes(row[1], "utf-8"), bytes(row[2], "utf-8")]
         c.execute("INSERT INTO REGION (R_REGIONKEY, R_NAME, R_COMMENT) VALUES (?, ?, ?);",to_db)
 elif cont == 'supplier.csv':
    with open('supplier.csv','r') as csvReader:
        reader = csv.reader(csvReader, delimiter= '|')
        for row in reader:
         to_db = [bytes(row[0], "utf-8"), bytes(row[1], "utf-8"), bytes(row[2], "utf-8"), bytes(row[3],"utf-8"), bytes(row[4], "utf-8"), bytes(row[5], "utf-8"), bytes(row[6], "utf-8")]
         c.execute("INSERT INTO SUPPLIER (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) VALUES (?, ?, ?, ?, ?, ?, ?);",to_db)
 else:
    print ("The csv file name not recognized.")

conn.commit()

for cont in content:
 print(cont + "\n")
f.close()
print("Nation table is populated with csv file.")



