import time
import requests
import json
import mysql.connector
from mysql.connector import errorcode
from getpass import getpass
import urllib
from datetime import datetime
from decimal import Decimal

DB_NAME = 'kite'
cnx = mysql.connector.connect(user='kite', password='test', database=DB_NAME)
cursor = cnx.cursor()

str = ''

symbols = ['500820.BSE', '532215.BSE', 'BAJAJ-AUTO.BSE', 'BAJFINANCE.BSE', '532978.BSE', '532454.BSE', '500124.BSE', 'HCLTECH.BSE', '500180.BSE', '540777.BSE', 'HINDUNILVR.BSE', '532174.BSE', '532187.BSE', 'INFY.BSE',
           'ITC.BSE', '500247.BSE', 'LT.BSE', 'M&M.BSE', 'MARUTI.BSE', 'NESTLEIND.BSE', 'NTPC.BSE', 'ONGC.BSE', 'POWERGRID.BSE', 'RELIANCE.BSE', 'SBIN.BSE', '524715.BSE', 'TCS.BSE', '532755.BSE', '500114.BSE', 'ULTRACEMCO.BSE']
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
        "ITC.BSE": "ITC_Limited",
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

final=[]
for symbol in symbols:
    thisdata = {}
    thisdata["Meta Data"] = {"2. Symbol": name[symbol]}
    str = """ SELECT * FROM kite."""+name[symbol]+""";"""
    cursor.execute(str)
    temp = {}
    for (refresh, open, high, low, close) in cursor:
        #thisdata.append({"refresh":refresh, "open":open,"high":high,"low":low,"close":close})
        #print("{} {} {} {} {}".format(refresh, open, high, low, close))
        temp[refresh.strftime("%x")] = {"1. open": "{0:f}".format(open), "2. high": "{0:f}".format(
            high), "3. low": "{0:f}".format(low), "4. close": "{0:f}".format(close)}
    thisdata["Time Series (Daily)"] = temp
    #print(thisdata)
    final.append(thisdata)

print(json.dumps(final))
cursor.close()
cnx.close()
