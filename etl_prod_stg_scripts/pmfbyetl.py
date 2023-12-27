
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
import warnings
warnings.filterwarnings('ignore')
# LOGGING CONFIGURATION
log = logging.getLogger()

class ccePmfbyETL():
    def __init__(self,engine,conn,cur):

        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.lgd_codes=None

    def getPmfbyData(self,season,year):

        try:
            log.info(f"Starting Fetching AY TY data for {season} {year}")
            ay_ty_df= pd.DataFrame()
            for lgd_code in self.lgd_codes:
                url = f"https://pmfby.gov.in/api/v1/cce/cce/ayTyDataSharing?season={season}&year={year}&districtLgdCode={lgd_code}&authToken=E36A8933-5D53-4340-A15F-87CA3B163675"
                print(url)
                response = requests.request("GET", url)
                if response.json()["data"]:
                    df = pd.DataFrame.from_records(response.json()['data'])
                    df = df.drop_duplicates()
                    # if 'ayUnit' not in df.columns:
                    df = df.replace('NULL',None)
                    df["Season"]=season
                    df["Year"]=year
                    df['request_date'] = str(datetime.today().date())
                    df['src_lgd_district_code'] = lgd_code
                    df.to_sql('stg_pmfby_ay_ty_suseel',self.engine, schema="staging", if_exists="append", index=False)
        
            return {'success':True }
        
        except Exception as e:
            log.critical('Error in '+self.getPmfbyData.__name__+f': {e}')
            log.info(response.json())
            return {'success' : False, 'message' : 'Error in '+self.getPmfbyData.__name__+f': {e}'}
    
    def getCceData(self,season,year):
        
        try:
            log.info(f'Started fetching CCE data for {season} {year}')
            cce_df = pd.DataFrame()
            for lgd_code in self.lgd_codes:
                url = f"https://pmfby.gov.in/api/v1/cce/cce/cceYieldDataSharing?season={season}&year={year}&districtLgdCode={lgd_code}&authToken=E36A8933-5D53-4340-A15F-87CA3B163675"
                response = requests.request("GET", url)
                if response.status_code == 404:
                    log.info(response.text)
                if response.json()['data']:
                    df = pd.DataFrame.from_records(response.json()['data'])
                    df = df.drop_duplicates()
                    df = df.replace('NULL',None)
                    df["Season"]=season
                    df["Year"]=year
                    df['request_date'] = str(datetime.today().date())
                    df['src_lgd_district_code'] = lgd_code
                    df.to_sql('stg_cce_suseel',self.engine, schema="staging", if_exists="append", index=False)
            log.info(f'Extracted data succesfully {season} {year}')
            return {'success':True}

        except Exception as e:
            log.critical('Error in '+self.getCceData.__name__+f': {e}')
            log.info(response.json())
            return {'success' : False, 'message' : 'Error in '+self.getCceData.__name__+f': {e}'}
        
    def getLgdCodes(self):

        try:
            log.info("Fetching LGD code from db.")
            lgd_code = pd.read_sql('''select distinct "District LGD Code"  from staging."districtLGDMaster" order by  "District LGD Code" ASC ''' , self.conn)
            log.info("LGD codes fethced successfully.")
            return {"success":True,"data":lgd_code["District LGD Code"]}
        except Exception as e:
            log.critical('Error in '+self.getLgdCodes.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.getLgdCodes.__name__+f': {e}'}

    def processPMFBYData(self):

        lgd_codes_resp=self.getLgdCodes()
        if not lgd_codes_resp["success"]:
            return {'success': False, 'message' : lgd_codes_resp['message']}
        self.lgd_codes=lgd_codes_resp["data"]
        # month,year = datetime.now().month,int(datetime.now().year)
        # if month <=6:
        #     years = [year-1]
        # else:
        #     years = [year-1,year]
        # years = (2021,2022, 2023)
        years = [2023]
        for year in years:
            for season in ["Kharif","Rabi"]:
                # cce_resp = self.getCceData(season,year)
                # if not cce_resp["success"]:
                #     log.info(cce_resp['message'])
                ay_ty_resp = self.getPmfbyData(season,year)
                if not ay_ty_resp['success']:
                    return {"success":False ,"message":ay_ty_resp["message"] }
        else:
            return {'success':True}
    
        
def pipeline_handler(connection,connection_type):

    log_file = f"Pmfby{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"PMFBY {connection_type} ETL job started at {start_time_stamp}")
    resp=ccePmfbyETL(engine,conn,cur).processPMFBYData()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type}  PMFBY ETL job ended at {end_timestamp}")
        subject = f"INFO :{connection_type} PMFBY ETL job status"
        body = f"{connection_type} PMFBY ETL job succeeded. Please check the attached logs for details."
    else:
        log.critical(f"{connection_type} PMFBY ETL job ended at {end_timestamp}")
        subject = f"CRITICAL : {connection_type} PMFBY ETL job status"
        body = f"{connection_type} PMFBY ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of PMFBY{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)

    # emailClient().send_mail(subject, body, log_file)
    # log.handlers.clear()
    # os.remove(log_file)
    conn.close()

# DB_CLIENTS = ('STAGING','PRODUCTION','DEV')
DB_CLIENTS = ['STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())

