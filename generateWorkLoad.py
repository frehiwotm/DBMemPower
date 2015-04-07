#!/usr/bin/env python3

import sqlite3
import csv
import time

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
def setupContent(conn, c):
    with open('csvfiles.txt','r') as f:
        content = f.readlines()
    for cont in content:
        cont = cont.strip('\n');
        if cont == 'nation.csv':
            with open('nation.csv','r') as csvReader:
                nationreader = csv.reader(csvReader, delimiter= '|')
                for row in nationreader:
                    executeInsert(c,'NATION',['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'],row)
        elif cont == 'lineitem.csv':
            with open('lineitem.csv','r') as csvReader:
                lineitemreader = csv.reader(csvReader, delimiter= '|')
                for row in lineitemreader:
                    executeInsert(c,'LINEITEM', ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
                                    'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE',
                                    'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'], row)
        elif cont == 'customer.csv':
            with open('customer.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    executeInsert(c, 'CUSTOMER', ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL','C_MKTSEGMENT', 'C_COMMENT'], row)
        elif cont == 'orders.csv':
            with open('orders.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    executeInsert(c, 'ORDERS', ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE',
                                            'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'], row)
        elif cont == 'part.csv':
            with open('part.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    executeInsert(c,'PART', ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER',
                                         'P_RETAILPRICE', 'P_COMMENT'], row)
        elif cont == 'partsupp.csv':
            with open('partsupp.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    executeInsert(c, 'PARTSUPP', ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'], row)
        elif cont == 'region.csv':
            with open('region.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    executeInsert(c, 'REGION', ['R_REGIONKEY', 'R_NAME', 'R_COMMENT'],row)
        elif cont == 'supplier.csv':
            with open('supplier.csv','r') as csvReader:
                reader = csv.reader(csvReader, delimiter= '|')
                for row in reader:
                    executeInsert(c, 'SUPPLIER', ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'], row)
        else:
            print ("The csv file name " + cont + " is not recognized.")
    conn.commit()
    conn.close()
    f.close()

#Main function
if __name__ == '__main__':
    connection, cursor = connect()
    setupDatabase(connection, cursor)
    setupContent(connection, cursor)

