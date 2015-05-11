#!/usr/bin/env python3

import sqlite3
import csv
import time
import argparse

# connect to the database
def connect():
    start_time = time.clock()
    conn = sqlite3.connect (":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
    return (conn, conn.cursor())

# Creating tables and the sql commands imported from external files
def setupDatabase(conn, c):
    with open('tpch-create.sql') as mytables:
        schema = mytables.read()
        mytables.close()
    # The multiple SQL commands split at ';' and executed separately
    sqlCommands = schema.split(';')
    for command in sqlCommands:
        # try:
        c.execute(command)
        # except sqlite3.OperationalError as e:
         #   print("command skipped:", e)
    conn.commit()

# Populating the database , i.e., execute insert statment.
def executeInsert(cursor, table, cols, row):
    def convertValue(value): 
        try:
            float(value)
            return value
        except ValueError:
            return "'%s'" % value
        
    qry = "INSERT INTO " + table
    qry += ' (' + ','.join(cols) + ') VALUES (' + ','.join([convertValue(row[i]) for i in range(len(cols))]) + ');'
    cursor.execute(qry)
    
# Importing the content which will be inserted to the database from CSV files.
def setupContent(conn, c, maxRows):
    
    with open('csvfiles.txt', 'r') as f:
        fileNames = f.readlines()
        
    for fileName in fileNames:
        fileName = fileName.strip()
        
        # ignore empty filenames
        if len(fileName) == 0 or fileName is None: 
            continue
        
        # open the file
        with open(fileName, 'r') as csvReader:
            inputReader = csv.reader(csvReader, delimiter='|')
            
            count = 0
            for row in inputReader:
                if fileName == 'nation.csv':
                    executeInsert(c, 'NATION', ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'], row)

                elif fileName == 'lineitem.csv':
                    executeInsert(c, 'LINEITEM', ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
                                    'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE',
                                    'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'], row)
                elif fileName == 'customer.csv':
                    executeInsert(c, 'CUSTOMER', ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY',
                                                  'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'], row)
                    
                elif fileName == 'orders.csv':
                    executeInsert(c, 'ORDERS', ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE',
                                            'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'], row)
                    
                elif fileName == 'part.csv':
                    executeInsert(c, 'PART', ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER',
                                         'P_RETAILPRICE', 'P_COMMENT'], row)
                elif fileName == 'partsupp.csv':
                    executeInsert(c, 'PARTSUPP', ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY',
                                                  'PS_SUPPLYCOST', 'PS_COMMENT'], row)
                elif fileName == 'region.csv':
                    executeInsert(c, 'REGION', ['R_REGIONKEY', 'R_NAME', 'R_COMMENT'], row)
                elif fileName == 'supplier.csv':
                    executeInsert(c, 'SUPPLIER', ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'], row)    
                else:
                    raise RuntimeError("Unknown csv data file %s" % fileName)

                count += 1
                
                # max number of rows imported, break earlier                
                if count == maxRows: 
                    break
        
    conn.commit()
    
def queryContent(conn): 
    
    c = conn.cursor()               
    while True:
        with open('workload.sql') as queries:
            schema = queries.read()
            
        # The multiple SQL commands split at ';' and executed separately
        count = 0;
        for query in schema.split(';'):
            if query is '\n': 
                continue
            count += 1
            
            try:
                c.execute(query)
                print("The result of the " + str(count) + " query:")	
                print(c.fetchall())
            except sqlite3.OperationalError as e:
                print("Query number " + str(count) + ":" + '"' + query + '"' + " not executed:", e)
    
    conn.commit()

# Main function
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="fix imported row numbers ")
    parser.add_argument("x", type=int, help="the row number")
    args = parser.parse_args()

    row = args.x
    connection, cursor = connect()
    setupDatabase(connection, cursor)
    setupContent(connection, cursor, row)
    queryContent(connection)
    connection.close()
