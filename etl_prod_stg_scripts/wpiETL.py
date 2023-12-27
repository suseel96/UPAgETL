# ----------------------------------------------------------------

#          Wholesale Price Index ETL JOB

# -----------------------------------------------------------------
# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import pandas as pd
import numpy as np
import pytz
from dotenv import load_dotenv
from etl_utils.utils import emailClient,dbClients,commonUtills
import logging
from datetime import timedelta, datetime
import requests
from datetime import date
import time
import warnings
warnings.filterwarnings('ignore')
# LOGGING CONFIGURATION
log = logging.getLogger()

class wpiETL:

    def __init__(self,engine,conn,cur):

        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_TABLE_NAME="stgWPI"
        self.DB_STAGING_SCHEMA="staging"

    @commonUtills.handleException(log)
    def get_wpi_data(self):
        log.info("Extracting WPI data.")
        url = "https://eaindustry.nic.in/webapi/apiagriwpi.asp"
        payload = {}
        headers = {
        }

        response = requests.request("GET", url, headers=headers, data=payload,verify=False)
        df = pd.DataFrame.from_records(response.json())
        log.info("WPI data extracted successfully.")
        return {"success":True ,"data":df}
        
    @commonUtills.handleException(log)  
    def get_dates_to_genrate(self):
        log.info("Genrating dates.")
        self.cur.execute("""select max("MonthCode") from staging."stgWPI" """)
        date=self.cur.fetchone()[0]
        start=f'{date[:4]}-{date[4:]}-01'
        end=datetime.today()
        dates=pd.date_range(start,end,freq='m')
        dates=['INDX'+x.strftime('%m%Y') for x in dates]
    
        if dates[1:]:
            log.info("Dates genrated successfully.")
            return {'success':True,'data':dates[1:]}
        
        else :
            log.info("No dates to genrate.")
            return {"success":False,"message":"No dates to genrate"}
            
    @commonUtills.handleException(log)
    def get_required_data(self,dates,df):
        log.info("Removing unrequired WPI columns")
        log.info(f"keeping only {dates} WPI columns")
        req_columns=['COMM_NAME', 'COMM_CODE','COMM_WT']
        avialable_dates=[date for date in dates if date in df.columns]
        if avialable_dates:
            log.info("Removed unrequired WPI columns successfully")
            return {"success":True,"data":df[req_columns+avialable_dates]}
        else :
            log.info(f"Data not avilable for {dates}")
            return {"success":False,"message":f"Data not available for {dates}"}
            
    @commonUtills.handleException(log)       
    def transform_data(self,df):
        log.info("Transforming data")
        df = df.melt(id_vars = ['COMM_NAME', 'COMM_CODE', 'COMM_WT'],value_name='WPI')
        df['Month'] = df['variable'].str[4:6]
        df['Year'] = df['variable'].str[6:]
        df["MonthCode"]=df["Year"]+df["Month"]
        df['RequestDate'] = datetime.today().strftime('%Y-%m-%d')
        df = df[['COMM_NAME', 'COMM_CODE','Year','Month','COMM_WT', 'WPI','RequestDate','MonthCode']]
        log.info("Data transformed successfully")
        
        return {'success':True,'data':df}
    
    @commonUtills.handleException(log)        
    def loadDataToStaging(self,df):
        try:
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
            self.conn.commit()
            log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            return {'success' : True}
        
        except Exception as e:
            log.critical('Error in '+self.loadDataToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToStaging.__name__+f': {e}'}
        
    @commonUtills.handleException(log)
    def loadDataToReporting(self):
        log.info(f'Loading latest data into reporting."RptWPI"')
        self.cur.execute( """
            select "QueryText" from reporting."ETLFlowMaster" em where "DataSource" ='WPI' and "SeqNo" =1 and "UpdateMode" ='Delta' """)
        querytext= self.cur.fetchall()[0][0]
        self.cur.execute(f'''{querytext}''')
        self.conn.commit() 
        log.info(f'Latest data loaded into reporting."RptWPI"')
        self.conn.commit()
        return {'success' : True}

    def startwpiETL(self):
        
        dates_to_gen_resp=self.get_dates_to_genrate()
        if not dates_to_gen_resp["success"]:
            return {"success":False ,"message":dates_to_gen_resp["message"] } 
           
        wpi_data_resp=self.get_wpi_data() 
        if not wpi_data_resp["success"]:
            return {"success":False ,"message":wpi_data_resp["message"] } 
        
        required_data_resp=self.get_required_data(dates=dates_to_gen_resp["data"],df=wpi_data_resp["data"])
        if not required_data_resp["success"]:
            return {"success":False ,"message":required_data_resp["message"] }
        
        transform_data_resp=self.transform_data(required_data_resp["data"])
        if not transform_data_resp["success"]:
            return {"success":False ,"message":transform_data_resp["message"] }
        
        load_data_to_staging_resp = self.loadDataToStaging(transform_data_resp["data"])
        if not load_data_to_staging_resp['success']:
            return {'success': False, 'message' : load_data_to_staging_resp['message']} 
        
        load_data_to_reporting_resp=self.loadDataToReporting()
        if not load_data_to_reporting_resp["success"]:
            return {"success":False,"message":load_data_to_reporting_resp['message']}
        
        return {"success":True}
    
def pipeline_handler(connection,connection_type):
    log_file = f"WPI{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"WPI ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = wpiETL(engine,conn,cur).startwpiETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} WPI ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} WPI ETL job status"
        body = f"{connection_type} WPI ETL job succeeded. Please check the attached logs for details."
    else:
        log.critical(f"{connection_type} WPI ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} WPI ETL job status'
        body = f"{connection_type} WPI ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of WPI{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())