
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
import logging
from datetime import timedelta, datetime
import requests
from etl_utils.utils import dbClients,emailClient,commonUtills
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

class fbilETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
    def maxDate(self):
        self.cur.execute('''
                       select left(max("processRunDate"),10) from staging."fbilExchangeRates"
        ''')
        max_date=self.cur.fetchall()[0][0]
        max_date = datetime.strptime(max_date, '%Y-%m-%d')
        max_date = max_date+timedelta(days=1)
        return max_date.strftime("%Y-%m-%d")
    def loadToStaging(self,fromDate,toDate):
        try:
            url = f"https://www.fbil.org.in/wasdm/refrates/fetchfiltered?fromDate={fromDate}&toDate={toDate}&authenticated=false"
            response = requests.request("GET", url)
            if  not response.json():
                log.critical('Data Not avaliable')
                return {'success' : False, 'message' : 'Data not avaliable'}
            else:
                df = pd.DataFrame.from_records(response.json())
                df['displayTime'] = pd.to_datetime(df['displayTime'])
                df.to_sql('fbilExchangeRates',self.engine, schema="staging", if_exists="append", index=False)
                log.info(f'Avaliable Data between {fromDate} to {toDate} loaded into staging')
                return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.loadToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadToStaging.__name__+f': {e}'}
        
    def updateFormulatedFBIL(self):
        try :
            self.cur.execute("""select "QueryText"  from reporting."ETLFlowMaster"  where "DataSource"='FBIL' ;""")
            self.cur.execute(self.cur.fetchall()[0][0])
            self.conn.commit()
            log.info("Data loaded from fbilExchangeRates to  Formulated fbilExchangeRates")
            return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.updateFormulatedFBIL.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.updateFormulatedFBIL.__name__+f': {e}'}
    def startFbilETL(self):
        fromDate=self.maxDate()
        toDate=datetime.now().date()
        loading_data=self.loadToStaging(fromDate,toDate)
        if loading_data['success']:
            self.updateFormulatedFBIL()
            return {'success':True}
        else:
            return {'success':False,'message':loading_data['message']}
        
def pipeline_handler(connection,connection_type):
    log_file = f"fbil{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))

    log.info(f"FBIL ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp=fbilETL(engine,conn,cur).startFbilETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))

    if resp['success']:
        log.info(f"{connection_type}  FBIL ETL job ended at {end_timestamp}")
        subject = f"INFO :{connection_type} FBIL ETL job status"
        body = f"{connection_type}  FBIL ETL job succeeded. Please check the attached logs for details."
    elif 'Data not avaliable' in  resp['message']:
        log.info(f"{connection_type} FBIL ETL job ended at {end_timestamp}")
        subject = f"INFO :{connection_type} FBIL ETL job status"
        body = f"{connection_type} FBIL ETL job succeeded No Data Found. Please check the attached logs for details."
        conn.close()
        os.remove(log_file)
        os._exit()
    else:
        log.critical(f"{connection_type} FBIL ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f"CRITICAL : {connection_type} FBIL ETL job status"
        body = f"{connection_type} FBIL ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60

    log.info(f"Execution time of fbil{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())

