# ----------------------------------------------------------------

#                 NMEO DAILY ETL JOB

# -----------------------------------------------------------------

# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import pytz
from dotenv import load_dotenv
from etl_utils.utils import dbClients,emailClient,commonUtills
import logging
from datetime import timedelta, datetime
import requests
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class nmeoETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgCPONMEO'

    @commonUtills.handleException(log)
    def __getDates(self):
        self.cur.execute('''
                    select date(max("Date_YYYYMMDD")) from staging."stgCPONMEO"
                ''')
        max_date=self.cur.fetchall()[0][0]
        # max_date=datetime.strptime('2023-10-10',"%Y-%m-%d").date()
        date_range = pd.date_range(start=(max_date+timedelta(days=1)), end = datetime.now().date()-timedelta(days=1),freq='B')
        date_range=[date.date() for date in date_range]
        return {'success':True,'data':date_range}

    @commonUtills.handleException(log)    
    def __getData(self,date):
        log.info(f'Fetching data for {date}')
        url = f"https://nfsm.gov.in/services/cponmeo.asmx/GetCPOPrice_Datewise?Date_YYYYMMDD={date}"
        response = requests.request("GET", url)
        if response.json()['status'] == 'Success':
            df=pd.DataFrame(response.json()['data'])            
            log.info(f'Data fetched for {date}')
            return {'success':True,'data':df}
        else :
            log.info(f'Data not found for {date}')
            return {'success':False,'message':f'Data not found for {date}'}
    
    @commonUtills.handleException(log)
    def __loadDataToStaging(self,df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        self.conn.commit()
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        return {'success' : True}

    @commonUtills.handleException(log)
    def _startNmeoETL(self):
        dates_resp=self.__getDates()
        if not dates_resp["success"]:
            return dates_resp
        dates=dates_resp["data"]
        final_df=pd.DataFrame()
        for date in dates:
            get_data_resp=self.__getData(date)
            if not get_data_resp['success']:
                break
            final_df=pd.concat([final_df,get_data_resp["data"]],ignore_index=True)

        if not final_df.empty:
            load_data_to_staging_resp = self.__loadDataToStaging(final_df)
            if not load_data_to_staging_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']}

        date_diff=(dates[-1]-date).days
        if date_diff>3: 
            return {'success' : False, 'message' : get_data_resp['message']+f". Not getting data from {date_diff} days."}
        
        return {"success":True}

def pipeline_handler(connection,connection_type):
    # connection_type=str(connection).split(' ')[2][18:-2]
    log_file = f"nmeo{(connection_type)}ETL.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"NMEO ETL job triggered at {start_time_stamp}") 
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    
    resp = nmeoETL(engine,conn,cur)._startNmeoETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} NMEO ETL JOB Ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f'INFO : {connection_type} NMEO ETL job status'
        body = f'{connection_type} NMEO ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"{connection_type} NMEO ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f'CRITICAL :{connection_type} NMEO ETL job status'    
        body = f"{connection_type} NMEO ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of nmeo{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())


