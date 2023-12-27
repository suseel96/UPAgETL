# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import pytz
from etl_utils.utils import dbClients,fileUploadUtils,emailClient,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import requests
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

def funcCall(cur, conn):
    try:
        log.info('''Executing SQL Procs''')
        cur.execute(''' call upag.sp_checkdtlatestestimatedata_v1() ''')
        conn.commit()
        return {'success': True}
    except Exception as e:
        log.critical('Error in '+funcCall.__name__+f': {e}')
        return {'success': False, 'message' : 'Error in '+funcCall.__name__+f': {e}'}

def pipeline_handler(connection,connection_type):
    log_file = f"dynamicdatatriangulationv1{(connection_type).lower()}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f" Dyanmic data triangulation reports v2 ETL job triggered at {start_time_stamp}")
    connect_db_resp = connection()
    time.sleep(10)
    if not connect_db_resp['success']:
        return {'success' : False, 'message' : connect_db_resp['message']}
    engine = connect_db_resp['engine']
    conn = connect_db_resp['conn']
    cur = connect_db_resp['cur']
    resp = funcCall(cur, conn)
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} Dynamic data triangulation V1 job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} Dynamic data triangulation job v1 status"
        body = f"{connection_type} Dynamic data triangulation job v1 succeeded. Please check the attached logs for details."
    else:
        log.critical(f"{connection_type} Dynamic data triangulation V1 job ended at {end_timestamp}")
        subject = f"CRITICAL : {connection_type} Dynamic data triangulation job v1 status"
        body = f"{connection_type} Dynamic data triangulation job v1 failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of dynamicdatatriangulation v1 {(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)

    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
# start_time = time.time()
# pipeline_handler(dbClient().connectToStagingDb)
# start_time = time.time()
# pipeline_handler(dbClient().connectToProductionDb)
DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())

