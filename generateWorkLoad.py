#!/usr/bin/env python3

import sqlite3
import csv
import time
import argparse

#connect to the database
def connect():
    start_time = time.clock()
    conn = sqlite3.connect (":memory:")
    return (conn, conn.cursor())

#Creating tables and the sql commands imported from external files
def setupDatabase(conn, c):
    with open('tpch-create.sql') as mytables:
        schema = mytables.read()
        mytables.close()
    #The multiple SQL commands split at ';' and executed separately
    sqlCommands = schema.split(';')
    for command in sqlCommands:
        try:
            c.execute(command)
        except sqlite3.OperationalError as e:
            print("command skipped:", e)
    conn.commit()

#Populating the database , i.e., execute insert statment.
def executeInsert(cursor, table, cols, row):
    to_db = [bytes(row[i], "utf-8") for i in range(len(cols))]
    qry = "INSERT INTO " + table
    qry += ' (' + ','.join(cols) + ') VALUES (' + ','.join(['?']*len(cols)) + ');'
    cursor.execute(qry, to_db)

#Importing the content which will be inserted to the database from CSV files.
def setupContent(conn, c, rownr):
    with open('csvfiles.txt','r') as f:
        content = f.readlines()
    for cont in content:
        if cont  is '\n' or None: continue 
        count = 0
        cont = cont.strip('\n');
        if cont == 'nation.csv':
            with open('nation.csv','r') as csvReader:
                nationreader = csv.reader(csvReader, delimiter= '|')
                for row in nationreader:
                    if count == rownr: break
                    executeInsert(c,'NATION',['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'],row)
                    count+=1
        elif cont == 'lineitem.csv':
            with open('lineitem.csv','r') as csvReader:
                lineitemreader = csv.reader(csvReader, delimiter= '|')
                for row in lineitemreader:
                    if count == rownr: break
                    executeInsert(c,'LINEITEM', ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
                                    'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE',
                                    'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'], row)
                    count+=1
        elif cont == 'customer.csv':
            with open('customer.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    if count == rownr: break
                    executeInsert(c, 'CUSTOMER', ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL','C_MKTSEGMENT', 'C_COMMENT'], row)
                    count+=1
        elif cont == 'orders.csv':
            with open('orders.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    if count == rownr: break
                    executeInsert(c, 'ORDERS', ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE',
                                            'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'], row)
                    count+=1
        elif cont == 'part.csv':
            with open('part.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    if count == rownr: break
                    executeInsert(c,'PART', ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER',
                                         'P_RETAILPRICE', 'P_COMMENT'], row)
                    count+=1
        elif cont == 'partsupp.csv':
            with open('partsupp.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    if count == rownr: break
                    executeInsert(c, 'PARTSUPP', ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'], row)
                    count+=1
        elif cont == 'region.csv':
            with open('region.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    if count == rownr: break
                    executeInsert(c, 'REGION', ['R_REGIONKEY', 'R_NAME', 'R_COMMENT'],row)
                    count+=1
        elif cont == 'supplier.csv':
            with open('supplier.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    if count == rownr: break
                    executeInsert(c, 'SUPPLIER', ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'], row)
                    count+=1
        else:
            print ("The csv file name " + cont + " is not recognized.")
    '''
    q = "SELECT * FROM PARTSUPP WHERE PS_SUPPLYCOST > 500;"
    c.execute(q)
    print(c.fetchall())
    ''' 

    while True:
        with open('workload.sql') as queries:
            schema = queries.read()
            queries.close()
        #The multiple SQL commands split at ';' and executed separately
        count = 0;
        sqlqueries = schema.split(';')
        for query in sqlqueries:
            if query is '\n': continue
            count+=1
            try:
                c.execute(query)
                print("The result of the " + str(count) + " query:")	
                print(c.fetchall())
            except sqlite3.OperationalError as e:
                print("Query number " + str(count) + ":" + '"' + query + '"' +  " not executed:", e)

    conn.commit()
    conn.close()
    f.close()

#Main function
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = "fix imported row numbers ")
    parser.add_argument("x", type = int, help = "the row number")
    args = parser.parse_args()

    row = args.x
    connection, cursor = connect()
    setupDatabase(connection, cursor)
    setupContent(connection, cursor, row)

