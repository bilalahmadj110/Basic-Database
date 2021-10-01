import pandas as pd
import re
import numpy as np
import datetime
from sqlalchemy import create_engine

EXCEL_SHEET = 'ExcelFile.xlsx'

server = 'localhost'
username = 'sa'
password = 'XXXXXXXXX'
database = 'XXXXXXX'



print ("Connecting to SQL Server", end=' - ')
engine = create_engine('mssql+pyodbc://%s:%s@%s/%s?driver=ODBC+Driver+17+for+SQL+Server' % \
                       (username, password, server, database),echo_pool=False, \
                       pool_timeout=30, pool_pre_ping=True, pool_recycle=3600)
print ("(SUCCESS)\n")


print (f"Reading excel \"{EXCEL_SHEET}\"", end=" - ")
excel = pd.ExcelFile(EXCEL_SHEET)
sheets = excel.sheet_names
print (f"(Found {len(sheets)} sheet(s))")




for sheet_name in sheets:
    print (f"Reading sheet \"{sheet_name}\"", end=" - ")
    read_sheet = excel.parse(sheet_name)
    print ("(Done)")

    

    table = f'sheet_{sheet_name}'.replace(' ', '_')
    table = str(re.sub('[^A-ZÜÖÄa-z0-9^_]+', '', table.lower())).strip()
    print (f"  Exporting to table \"{table}\"", end=" - ")
    read_sheet.to_sql(name=table, con=engine, if_exists='append', index=False)
    print ("(Done)")

    





