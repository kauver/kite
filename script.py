import time
import requests
import json
import mysql.connector
from mysql.connector import errorcode
from getpass import getpass
import urllib
from datetime import datetime


cnx = mysql.connector.connect(user='kite',password='test')
cursor = cnx.cursor()
DB_NAME = 'kite'
str=''

apikey = '3KYYJ04S8PXDYJ9O.1..5'
symbols = ['500820.BSE', '532215.BSE', 'BAJAJ-AUTO.BSE', 'BAJFINANCE.BSE', '532978.BSE', '532454.BSE', '500124.BSE', 'HCLTECH.BSE', '500180.BSE', '540777.BSE', 'HINDUNILVR.BSE', '532174.BSE', '532187.BSE', 'INFY.BSE', 'ITC.BSE', '500247.BSE', 'LT.BSE', 'M&M.BSE', 'MARUTI.BSE', 'NESTLEIND.BSE', 'NTPC.BSE', 'ONGC.BSE', 'POWERGRID.BSE', 'RELIANCE.BSE', 'SBIN.BSE', '524715.BSE', 'TCS.BSE', '532755.BSE', '500114.BSE', 'ULTRACEMCO.BSE']
#symbols = ['500820.BSE']
name = {"500820.BSE": "Asian_Paints_Limited",
        "532215.BSE": "Axis_Bank_Limited",
        "BAJAJ-AUTO.BSE": "Bajaj_Auto_Limited",
        "BAJFINANCE.BSE": "Bajaj_Finance_Limited",
        "532978.BSE": "Bajaj_Finserv_Ltd",
        "532454.BSE": "Bharti_Airtel_Limited",
        '500124.BSE': "Dr_Reddys_Laboratories_Limited",
        "HCLTECH.BSE": "HCL_Technologies_Limited",
        "500180.BSE": "HDFC_Bank_Limited",
        "540777.BSE": "HDFC_Life_Insurance_Company_Limited",
        "HINDUNILVR.BSE": "Hindustan_Unilever_Limited",
        "532174.BSE": "ICICI_Bank_Limited",
        "532187.BSE": "IndusInd_Bank_Limited",
        "INFY.BSE": "Infosys_Limited",
        "ITC.BSE":"ITC_Limited",
        "500247.BSE": "Kotak_Mahindra_Bank_Limited",
        "LT.BSE": "Larsen_And_Toubro_Limited",
        "M&M.BSE": "Mahindra_And_Mahindra_Limited",
        "MARUTI.BSE": "Maruti_Suzuki_India_Limited",
        "NESTLEIND.BSE": "Nestle_India_Limited",
        "NTPC.BSE": "NTPC_Limited",
        "ONGC.BSE": "Oil_and_Natural_Gas_Corporation_Limited",
        "POWERGRID.BSE": "Power_Grid_Corporation_of_India_Limited",
        "RELIANCE.BSE": "Reliance_Industries_Limited",
        "SBIN.BSE": "State_Bank_of_India",
        "524715.BSE": "Sun_Pharmaceutical_Industries_Limited",
        "TCS.BSE": "Tata_Consultancy_Services_Limited",
        "532755.BSE": "Tech_Mahindra_Limited",
        "500114.BSE": "Titan_Company_Limited",
        "ULTRACEMCO.BSE": "UltraTech_Cement_Limited",
        }
count=0

def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)

for symbol in symbols:
    print(name[symbol])
    
    str="""
CREATE TABLE `"""+name[symbol]+"""` (
    `refresh` date PRIMARY KEY,
    `open` decimal(12,4),
    `high` decimal(12,4),
    `low` decimal(12,4),
    `close` decimal(12,4)
)
"""
   
    try:
        
        cursor.execute(str)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            pass
        else:
            print(err.msg)
    else:
        pass


    #print(str)
    
    str = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol='+urllib.parse.quote(symbol)+'&apikey='+apikey
    #print (str)
    response = requests.get(str)
    
    data=response.json()
    #print(data['Time Series (Daily)'])
    findata = data['Time Series (Daily)']
    for entry in data['Time Series (Daily)']:
        #print(datetime.strptime(entry,"%Y-%m-%d").date())
        #print(findata[entry])
        insert_data = {'refresh': datetime.strptime(entry, "%Y-%m-%d").date(), 'open':findata[entry]['1. open'], 'high':findata[entry]['2. high'], 'low':findata[entry]['3. low'], 'close':findata[entry]['4. close']}
        insert_string="""
INSERT IGNORE INTO """+name[symbol]+""" (refresh,open,high,low,close)
VALUES
    (%(refresh)s, %(open)s, %(high)s, %(low)s ,%(close)s)"""
        #print(insert_string,insert_data)
        cursor.execute(insert_string, insert_data)
        cnx.commit()
    count+=1  
    if count==5:
        count=0
        time.sleep(60)


cursor.close()
cnx.close()
