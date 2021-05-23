from prettyprinter import pprint
import pandas as pd
import pytest
from bsedata.bse import BSE
from tqdm import tqdm
import psycopg2
import os
import psycopg2
import numpy as np
import psycopg2.extras as extras
from io import StringIO

table_name = "daily_data"
b = BSE(update_codes=True)
strips_list = pd.read_csv('strips/strips_list.csv')
strips = strips_list['Security Code']
missing=[]
cols = [
    'currentValue',
    'change',
    'pChange',
    'updatedOn',
    'priceBand',
    'securityID',
    'scripCode',
    'group',
    'faceValue',
    'industry',
    'previousClose',
    'previousOpen',
    'dayHigh',
    'dayLow',
    '52weekHigh',
    '52weekLow',
    'weightedAvgPrice',
    'totalTradedValue',
    'totalTradedQuantity',
    '2WeekAvgQuantity',
    'marketCapFull',
    'marketCapFreeFloat',
    'upperPriceBand',
    'lowerPriceBand']
strips = strips[:10]
df = pd.DataFrame(columns=cols)
for i,j in tqdm(strips.items()):
    i+=1
    try:
        q = b.getQuote(str(j))
        record_dict =  pd.DataFrame(dict((key,value) for key, value in q.items() if key in cols), index=[i])
        df = df.append(record_dict)
    except:
        missing.append(j)
    # pprint(q)
df = df[cols]
df = df.rename(columns={"group": "group_", "52weekHigh": "week52High","52weekLow":"week52Low",
                   "2WeekAvgQuantity":"Week2AvgQuantity"})
df.to_csv('market_test_data.csv')



con = psycopg2.connect(database="marketdata", user="vadith", password="", host="127.0.0.1", port="5432")
print("Database opened successfully")

cur = con.cursor()
cur.execute(
    """
CREATE TABLE IF NOT EXISTS {} (
    currentValue float8,
    change float8,
    pChange float8,
    updatedOn date,
    priceBand varchar,
    securityID varchar,
    scripCode varchar,
    group_ varchar,
    faceValue varchar,
    industry varchar,
    previousClose float8,
    previousOpen float8,
    dayHigh float8,
    dayLow float8,
    week52High float8,
    week52Low float8,
    weightedAvgPrice float8,
    totalTradedValue varchar,
    totalTradedQuantity varchar,
    Week2AvgQuantity varchar,
    marketCapFull varchar,
    marketCapFreeFloat varchar,
    upperPriceBand varchar,
    lowerPriceBand varchar
);

      """.format(table_name)
      )
print("Table created successfully")
con.commit()

def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")

execute_values(con, df, table_name)
con.close()
