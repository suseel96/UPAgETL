import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
import json
import pandas as pd
import numpy as np
import pytz
from etl_utils.utils import dbClients,emailClient,fileUploadUtils
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import requests
import warnings
warnings.filterwarnings('ignore')
import time

start = time.time()
# LOGGING CONFIGURATION
log = logging.getLogger()

# EMAIL CONFIGURATION
# EMAIL_SENDER,EMAIL_RECEIVER,EMAIL_PASSWORD = email_sender_receiver()
# EMAIL_SENDER = 'upagscripts@gmail.com'
# EMAIL_RECEIVER = 'mohit.chaniyal@otsi.co.in'
# EMAIL_PASSWORD = 'iesnszwcnzbmsrij'

class fciETL:
    def __init__(self,engine,conn,cur):
        self.year=datetime.now().year
        self.month=datetime.now().month
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgFCIStockTesting'
        
    def get_stock_data(self,year,month):
        url = "http://api.iisfm.nic.in/api/caputilstock"
        
        payload = json.dumps({
            "FromMonth": f"{month}",
            "FromYear": f"{year}",
            "ToMonth": f"{month}",
            "ToYear": f"{year}",
            "TokenNo": "305871611202212245678912",
            "AuthString": "{DoAaFW}9hZnc6Z9hZncywNTcyNQ==" })
        headers = {
          'Content-Type': 'application/json' }
        try :
            log.info(f"Started Fetching Data For {month}-{year} ")
            response = requests.request("POST", url, headers=headers, data=payload)
            df=pd.DataFrame(response.json()["OutputDetails"])
            df=df.astype({"MONTH":int,"YEAR":int})
            df["RequestDate"]=datetime.now().date()
            df["CapacityUOM"]="MT"
            df["StockUOM"]="Quintal KG"
            df["UtilizationUOM"]="%age"
            log.info(f"Data Successfully Fetched For {month}-{year}")
            return {"success":True,"data":df}
        except Exception as e:
            log.critical('Error in '+self.get_stock_data.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.get_stock_data.__name__+f': {e}'}
        
    def loadDataToStaging(self,df):
        try:
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            print(df)
            df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
            self.conn.commit()
            log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToStaging.__name__+f': {e}'}
    # def get_status(self,year,month,df_len):
    #     query=f"""SELECT count(*) FROM {self.DB_STAGING_SCHEMA}."{self.DB_STAGING_TABLE_NAME}" sfd  
    #         WHERE sfd."YEAR" = '{year}' AND sfd."MONTH" = '{month}';"""
    #     try:
    #         df=pd.read_sql_query(query,self.conn)
    #         if not df.empty:
    #             if df_len==df.values[0][0]:
    #                 return {"success":True ,"message":f"Data Already Up To Date For {month}-{year}","status":1}
    #             else :
    #                 return {"success":True,"message":f"Data Need To Be Updated For {month}-{year}","status":2}
    #     except Exception as e:
    #         log.critical('Error in '+self.get_status.__name__+f': {e}')
    #         return {'success' : False, 'message' : 'Error in '+self.get_status.__name__+f': {e}'}
    #     return {"success":True,"message":f"No Data Available For {month}-{year}","status":0}
    
    # def update_data(self,year,month,df):
    #     # query=f""" delete  from {self.DB_STAGING_SCHEMA}."{self.DB_STAGING_TABLE_NAME}" sfd where sfd."YEAR"={year} and sft."MONTH"={month}; """
    #     pass
    def startfciETL(self):
        try:
            log.info(f"FCIStock ETL job triggered at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
            stock_resp=self.get_stock_data(year=self.year,month=self.month-1)
            if not stock_resp["success"]:
                return stock_resp
            load_data_to_staging_resp = self.loadDataToStaging(stock_resp["data"])
            if not load_data_to_staging_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']} 
        # status_resp=self.get_status(self.year,self.month-1,len(stock_resp["df"]))
        # if not status_resp["success"]:
        #     return status_resp
        # if status_resp["status"]==0:
        #     # df.to_sql(f"{self.DB_STAGING_TABLE_NAME}",self.engine,schema=f"{self.DB_STAGING_SCHEMA}", if_exists="append", index=False, chunksize=100000)
        #     # log.info("Data Loaded To staging.stgFCIStock")
        #     pass
        # elif status_resp["status"]==1:
        #     return {"success":True,"message":status_resp["message"]}
        # elif status_resp["status"]==2:
        #     pass
        
            return {"success":True}
        except Exception as e:
            return {'success': False, 'message' : 'Error in '+self.startNmeoETL.__name__+f': {e}'}
    
def pipeline_handler(connection,connection_type):
    log_file = f"fciStock{connection_type}ETL.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    connect_to_db_resp = connection()
    connection_type=str(connection).split(' ')[1][9:-2] #staging or production
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp=fciETL(engine,conn,cur).startfciETL()
    if resp['success']:
        log.info(f" {connection_type}  fciStock ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f'INFO :  {connection_type}  fciStock ETL job status'
        body = f' {connection_type}  fciStock ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f" {connection_type}  fciStock ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f'CRITICAL : {connection_type}  fciStock ETL job status'
        body = f" {connection_type}  fciStock ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = time.time() - start_time   
    log.info(f"Execution time of  fciStock{connection_type}ETL is {str(total_time/60)} Minutes")
    # test=emailClient()
    # test.EMAIL_RECEIVER='mohit.chaniyal@otsi.co.in'
    # send_mail(subject, body, log_file, EMAIL_SENDER, EMAIL_RECEIVER, EMAIL_PASSWORD)
    log.handlers.clear()
    # os.remove(log_file)
    conn.close()
    
DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS[:1]:
    start_time = time.time()
    pipeline_handler(dbClients(client).connectToDb,client.title())