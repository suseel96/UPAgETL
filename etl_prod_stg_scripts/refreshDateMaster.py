# ----------------------------------------------------------------

#                 refreshDateMaster DAILY ETL JOB

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
from etl_utils.utils import emailClient,dbClients,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import requests
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class dateMasterETL():
    def __init__(self,cur,conn,engine):
        self.DB_REPORTING_SCHEMA = 'reporting'
        self.DB_REPORTING_VIEW_NAME = 'RptMIPDateMatser'
        self.cur = cur
        self.conn = conn
        self.engine = engine
            
    def refreshDateMasterMV(self):
        try:
            log.info(f'Refreshing materialized view {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_VIEW_NAME}')
            self.cur.execute(f'''
            refresh materialized view {self.DB_REPORTING_SCHEMA}."{self.DB_REPORTING_VIEW_NAME}"
            ''')
            log.info(f'Refreshed materialized view {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_VIEW_NAME}')
            self.cur.execute('''refresh materialized view reporting."mv_ZoneWiseDocWholeRetail" ''')
            log.info('Refreshed materialized view ZonewiseDocaWholeRetail')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.refreshDateMasterMV.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.refreshDateMasterMV.__name__+f': {e}'} 
       

    def startRefreshDateMasterMVETL(self):
        try:
            
            refresh_date_master_resp = self.refreshDateMasterMV()
            if not refresh_date_master_resp['success']:
                return {'success': False, 'message' : refresh_date_master_resp['message']}
            else:
                return {'success': True}
        except Exception as e:
            log.info(f"DateMaster ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
            return {'success': False, 'message' : 'Error in '+self.startRefreshDateMasterMVETL.__name__+f': {e}'}


def pipeline_handler(connection,connection_type):
    log_file = f"datemaster{(connection_type).lower()}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"DateMaster ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = dateMasterETL(cur,conn,engine).startRefreshDateMasterMVETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} DateMaster ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} DateMaster ETL job status"
        body = f"{connection_type} DateMaster ETL job succeeded. Please check the attached logs for details."
    else:
        log.critical(f"{connection_type} DateMaster ETL job ended at {end_timestamp}")
        subject = f"CRITICAL : {connection_type} DateMaster ETL job status"
        body = f"{connection_type} DateMaster ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of refreshdatemaster{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())

