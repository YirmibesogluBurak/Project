#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import json
import pyodbc
import datetime
import pandas as pd

assets=['BTC','ETH','ADA','BNB','USDT','LTC','WBTC','SHIB','LINK','ALGO','XTZ','THETA','FTM','PERP','FX']

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=T1LPRVT2834;'
                      'Database=Test;'
                      'Trusted_Connection=yes;')

for x in range(15):
    url='https://www.cryptingup.com/api/assets/'+assets[x]
    r = requests.get(url)
    data = r.json()
    assetId=data['asset']['asset_id']
    assetName=data['asset']['name']
    assetPrice=data['asset']['price']
    volume24h=data['asset']['volume_24h']
    change1h=data['asset']['change_1h']
    change24h=data['asset']['change_24h']
    change7d=data['asset']['change_7d']
    astatus=data['asset']['status']
    created=datetime.datetime.strptime(data['asset']['created_at'], '%Y-%m-%dT%H:%M:%S')
    updated=datetime.datetime.strptime(data['asset']['updated_at'], '%Y-%m-%dT%H:%M:%S.%f')
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
               (assetId,assetName,assetPrice,volume24h,change1h,change24h,change7d,astatus,created,updated,datetime.datetime.now()))
    conn.commit()

url='https://www.cryptingup.com/api/assets/USD/markets/?size=500'
r = requests.get(url)
data = r.json()

cursor = conn.cursor()

for item in data['markets']:
    markId=item['exchange_id']
    sym=item['symbol']
    base=item['base_asset']
    quote=item['quote_asset']
    priceUnc=item['price_unconverted']
    price=item['price']
    change24h=item['change_24h']
    spread=item['spread']
    volume24h=item['volume_24h']
    status=item['status']
    a_created=item['created_at']
    created=a_created[0:19]
    #updated=item['updated_at']
    #created=datetime.datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%S')
    updated=datetime.datetime.strptime(item['updated_at'], '%Y-%m-%dT%H:%M:%S.%f')
    cursor.execute("INSERT INTO markets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)",
                   (markId,sym,base,quote,priceUnc,price,change24h,spread,volume24h,status,
                    created,updated,datetime.datetime.now()))
conn.commit()


cursor = conn.cursor()
cursor.execute("DELETE FROM markets where base not in('BTC','ETH','ADA','BNB','USDT','LTC','WBTC','SHIB','LINK','ALGO','XTZ','THETA','FTM','PERP','FX')")
conn.commit()