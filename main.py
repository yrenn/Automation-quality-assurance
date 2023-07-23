import pyodbc 
import pandas as pd
import csv
import numpy as np
import snowflake.connector
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import datacompy
from datetime import datetime
import pytz
import logging



def loadingTarTable():
    url = URL(
    user='yuren',
    password='********',
    account='ZYWISZN-JU46823',
    warehouse='COMPUTE_WH',
    database='EDP',
    schema='SAW',
    role = 'ACCOUNTADMIN'
    )
    engine = create_engine(url)

    try:
        connection = engine.connect()
    
        logging.info('===== Get connection to snowflake =====')

    except Exception as e:
        logging.error('Fail to connect snowflake, please check the URL' +  str(e))

    return connection

def loadingSrcTable():
    driver = 'SQL Server'
    server = 'DESKTOP-A93UH7G'
    database = 'EDP'

    # connect to the local SQL Server database
    try:
        connection = pyodbc.connect(f'DRIVER={driver};'
                                f'SERVER={server};'
                                f'DATABASE={database};'
                                f'Trusted_Connection=yes;')
    
        logging.info('===== Get connection to sql server =====')

    except Exception as e:
        logging.error('Fail to connect sql server, please check the URL' +  str(e))

    return connection

def getSrcTable(connection, srcQuery, src_name):

    try:

        sql_query = pd.read_sql(srcQuery, connection) 

        df = pd.DataFrame(sql_query)

        logging.info("***** get table " + src_name + " from sql server *****")
    
    except Exception as e:
        logging.error('fail to read or run the sql query in sql server' + str(e))

    return df


def getTarTable(connection, tarQuery, tar_name):
    try:

        df = pd.read_sql(tarQuery, connection)

        logging.info("***** get table " + tar_name + " from sql server *****")

    except Exception as e:

        logging.error('fail to read or run the sql query in snowflake' + str(e))

    return df

def writeReport(src_name, tar_name, report):

    tz = pytz.timezone('America/TORONTO') 
    now = datetime.now(tz)
    time = now.strftime("%H_%M_%S")

    filename = 'Report_' + src_name + '_'+ time + '.txt'
    with open(filename, 'w') as f:
        f.write(report)
    
    logging.info("===== write comparation report for " + src_name + " =====")

def compareTables(src_connection, tar_connection):
    try:
        with open('config.csv', newline='\n') as csvfile:
            reader = csv.DictReader(csvfile,delimiter=',')
    except Exception as e:
        logging.error('Error: fail to read config.csv file', + str(e))
            
        for row in reader:
                
                src_name = row['SOURCETABLE']
                src_col = row['SRCCOL']
                tar_name = row['TARGETTABLE']
                tar_col = row['TGTCOL']
                src_que = ''' select ''' + src_col +''' from ''' + src_name + '''; 
                               '''
                tar_que = ''' select ''' + tar_col +''' from ''' + tar_name + '''; 
                               '''
                src_df = getSrcTable(src_connection, src_que, src_name)
                tar_df = getTarTable(tar_connection, tar_que, tar_name)
                # print(tar_que)
                # print(src_df)
                # print(tar_df.shape)

                compare = datacompy.Compare(src_df, tar_df, on_index=True)
                # compare = datacompy.Compare(src_df, tar_df)

                ##### how to solve the prime key              
                if(compare.matches()):
                    logging.info(src_name + ' tables are the same')

                else:
                    logging.info(src_name + ' tables are not same. Please see the report')
                
                # # create report
                report = compare.report()

                try: 
                    writeReport(src_name, tar_name, report)
                except Exception as e:
                    logging.error('Error: fail to write report' + str(e))
    return 0

                

src_connection = loadingSrcTable()
tar_connection = loadingTarTable()
compareTables(src_connection, tar_connection)
