import random, string
import pandas as pd
from urllib.parse import urlencode, quote_plus
import flask_login
import re
from bs4 import BeautifulSoup


server = 'localhost'
username = 'sa'
password = 'XXXXXXXXXXXX'
database = 'XXXXXXXXXXXX'
table = 'XXXXXXXXXXXX'

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, DateTime

engine = create_engine('mssql+pyodbc://%s:%s@%s/%s?driver=ODBC+Driver+17+for+SQL+Server' % (username, password, server, database),echo_pool=False, pool_timeout=30, pool_pre_ping=True, pool_recycle=3600)

def sanitize_total_hours(cell):
    if cell is None:
        return None
    try:
        cell = cell.split('x') 
        if len(cell) == 1:
            return float(re.sub("[^0-9.]", "", cell[0]))
        a = re.sub("[^0-9.]", "", cell[0]) 
        b = re.sub("[^0-9.]", "", cell[1])
        return float(a)*float(b) 
    except:
        return None
        
def create_bill_rate(row):
    hours = sanitize_total_hours(row['Total Hours']) 
    weekly = row['Weekly Pay']
    if hours is None or weekly is None:
        return None
    # if hours <= 40:
    if hours == 0:
        hours = 36
    return ((weekly + (weekly * 0.12)) / (0.725 * hours))

Q1 = f'''SELECT DISTINCT 
                    [Job Title]
                    ,[City]
                    ,[State]
                    ,[Hospital]
                    ,[Speciality]
                    ,[Discipline]
                    ,[Total Hours]
                    ,[Shift]
                    ,[Weekly Pay] 
                    ,NULL as [Openings]
                    ,[Duration]
                    ,[Job ID]
                    ,[Start Date]
                    ,[Posted Date]
            FROM [WebScraping].[dbo].[Vivian];'''
DF = pd.read_sql_query(Q1, engine)

if len(DF) != 0:
    DF['Bill Rate'] = DF.apply(create_bill_rate, axis=1)
    DF.index = DF.index * 2

Q2 = f'''SELECT DISTINCT 
        NULL as [Job Title]
        ,[City]
        ,[State]
        ,[Hospital]
        ,[Speciality]
        ,NULL as [Discipline]
        ,[Total Hours]
        ,[Shift]
        ,[Weekly Pay To] as [Weekly Pay]
        ,[Openings]
        ,NULL as [Duration]
        ,[Job ID]
        ,[Start Date]
        ,NULL as [Posted Date]
FROM [WebScraping].[dbo].[Aya_test];'''

DF2 = pd.read_sql_query(Q2, engine)

if len(DF2) != 0:
    DF2['Bill Rate'] = DF2.apply(create_bill_rate, axis=1)
    DF2.index = (DF2.index * 2) + 1
    
if len(DF) == 0:
    DF = DF2
elif len(DF2) == 0:
    pass
else: # in case DF2 and DF have valid length
    DF = pd.concat([DF, DF2]).sort_index()
  
  
print (f"Pushing data into {table}...", end=' - ')
DF.to_sql(table, engine, index=False, if_exists='replace')
print ("(Success)")