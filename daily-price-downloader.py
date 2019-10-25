import requests
from io import StringIO
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine
import pymysql

datestr = datetime.datetime.now().strftime("%Y%m%d")
#datestr = '20191021'

# 下載股價
r = requests.post('https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL')
if not r.text:
    print(datestr + " is a holiday.")
    exit(0)

# 整理資料，變成表格
df_stg = pd.read_csv(StringIO(r.text.replace("=", "")), 
            header=["證券代號" in l for l in r.text.split("\n")].index(True)-1)

# 整理一些字串：
#df_stg = df_stg.fillna(0)
#print(df_stg.head(30))

df = df_stg.apply(lambda s: pd.to_numeric(s.astype(str).str.replace(",", "").replace("+", "1").replace("-", "-1"), errors='coerce'))

df["證券名稱"] = df_stg["證券名稱"]
df["證券代號"] = df_stg["證券代號"]
df = df.rename(columns={"漲跌(+/-)":"漲跌"})

df.drop(df.columns[[16]], axis=1, inplace=True)
df = df.fillna(0)

def create_stock_tbl(tbl, cursor):
    sql = '''create table if not exists `{}` ( \
    `證券代號` text, \
    `證券名稱` text, \
    `成交股數` bigint(20) DEFAULT NULL, \
    `成交筆數` bigint(20) DEFAULT NULL, \
    `成交金額` bigint(20) DEFAULT NULL, \
    `開盤價` double DEFAULT NULL, \
    `最高價` double DEFAULT NULL, \
    `最低價` double DEFAULT NULL, \
    `收盤價` double DEFAULT NULL, \
    `漲跌` double DEFAULT NULL, \
    `漲跌價差` double DEFAULT NULL, \
    `最後揭示買價` double DEFAULT NULL, \
    `最後揭示買量` double DEFAULT NULL, \
    `最後揭示賣價` double DEFAULT NULL, \
    `最後揭示賣量` double DEFAULT NULL, \
    `本益比` double DEFAULT NULL, \
    `日期` DATETIME \
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 '''.format(tbl)
    #print(sql)
    cursor.execute(sql)

con = pymysql.connect(host="localhost", user="stockuser", passwd="tc22246", db="stock_tw")
cursor = con.cursor()
    
for index, row in df.iterrows():
    tbl = row['證券代號']
    
    create_stock_tbl(tbl, cursor)
    stock_row = row.values.tolist()
    stock_row = stock_row
    #print(stock_row)
    sql = '''insert into `{}` values ({}, {})'''.format(tbl, stock_row, 'NOW()').replace("[", "").replace("]", "")
    #print(sql)
    cursor.execute(sql)
    con.commit()
con.close()


