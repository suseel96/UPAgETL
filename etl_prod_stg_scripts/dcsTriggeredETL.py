# ----------------------------------------------------------------

#          DCS ETL job triggered based on JSON file 
#           received from SEEK API

# -----------------------------------------------------------------
#Imports
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import math
import pytz
from etl_utils.utils import dbClients,emailClient,fileUploadUtils,commonUtills
from etl_prod_stg_scripts.dcsETL import dcsETL
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import requests
import warnings
warnings.filterwarnings('ignore')
import time
import regex as re

# LOGGING CONFIGURATION
log = logging.getLogger()

file_name = sys.argv[1]


class dcsTriggeredETL:
    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.page_size = 10000
        
    def readDataFromFile(self):
        try:
            log.info(f'Reading data from {self.data_file[16:]}')
            with open(self.data_file, 'r') as data:
                file_data = json.load(data)
            log.info(f'Data read from {self.data_file[16:]} successsfully.')
            return {'success' : True, 'data' : file_data}
        except Exception as e:
            log.critical('Error in '+self.readDataFromFile.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.readDataFromFile.__name__+f': {e}'}
    
    def fetchRequestParams(self, tx_id):
        try:
            log.info(f'Fetching requested state code, year, season for tx_id : {tx_id}')
            self.cur.execute(f'''SELECT seek_api_request->'message'->'search_request'->0->'search_criteria'->'query'->'query_params'->0->'state_lgd_code' AS state_lgd_code,
                                seek_api_request->'message'->'search_request'->0->'search_criteria'->'query'->'query_params'->0->'year' AS year,
                                seek_api_request->'message'->'search_request'->0->'search_criteria'->'query'->'query_params'->0->'season' AS season
                                FROM staging.dcs_seekapi_metadata
                                where transaction_id = '{tx_id}' ''')
            res = self.cur.fetchall()[0]
            state = res[0]
            year = res[1]
            season = res[2]
            return {'success' : True, 'state' : state, 'year': year,'season':season}
        except Exception as e:
            log.critical('Error in '+self.fetchRequestParams.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.fetchRequestParams.__name__+f': {e}'} 
                                        
    def makeRequests(self, data):
        try:
            log.info(''' Requesting data in pages ''')
            if not data["is_validated"]:
                log.critical(f"Payload not validated for tx_id {self.data_file.split('/')[-1].split('.json')[0]}")
                return {'success' : False, 'message':'Payload not validated'}
            log.info(f"Payload validated for tx_id {self.data_file.split('/')[-1].split('.json')[0]}. Processing data for this transaction.")
            tx_id = self.data_file.split('/')[-1].split('.json')[0]
            request_params_resp = self.fetchRequestParams(tx_id)
            state = request_params_resp['state']
            year = request_params_resp['year']
            season = request_params_resp['season']
            if (
                data['validated_object']['payload']['message']['pagination']['page_size'] == 1) and (
                data['validated_object']['payload']['message']['pagination']['page_number'] == 1):
                    record_count = data['validated_object']['payload']['message']['pagination']['total_count']
                    page_count = math.ceil(record_count/self.page_size)
                    for page in range(page_count):
                        dcsETL(self.engine,self.conn,self.cur).requestSeekAPI(year = year, state = state, season = season, 
                                                                              page_size=self.page_size, page_num=page+1)
            else:
                total_df = pd.DataFrame()
                for i in data['validated_object']['payload']['message']['search_response']:
                    if i['data']['reg_records']:
                        df = pd.DataFrame(i['data']['reg_records'])
                        total_df = pd.concat([total_df, df], ignore_index=True)
                if total_df.empty:
                    return {'success' : True}
                total_df['state_code'] = state
                total_df = total_df[["state_code", "year", "survey_status", "crop_name", "crop_code", "district_name", 
                                     "village_name","village_lgd_code", "season", "irrigation_type", "sown_area", "area_unit"]]
                for _, row in total_df.iterrows():
                    self.cur.execute(f'''
                                    INSERT INTO staging.stgdcsdata (state_code, year , survey_status, crop_name, crop_code, district_name, 
                                    village_name, village_lgd_code, season, irrigation_type, sown_area, area_unit)
                                    VALUES (%(state_code)s, %(year)s, %(survey_status)s,%(crop_name)s,%(crop_code)s,%(district_name)s,%(village_name)s,
                                    %(village_lgd_code)s, %(season)s, %(irrigation_type)s, %(sown_area)s, %(area_unit)s)
                                    ON CONFLICT (state_code, year, survey_status, crop_name, crop_code, village_lgd_code, season, irrigation_type, area_unit, village_name, district_name) DO UPDATE 
                                    SET sown_area = EXCLUDED.sown_area;
                                     ''', row.to_dict())
                # total_df.to_sql('stgdcsdata', self.engine, schema='staging', if_exists="append", index=False, chunksize=50000)
                self.conn.commit()
                self.cur.close()
            log.info(f"Data for tx_id {self.data_file.split('/')[-1].split('.json')[0]} inserted into db.")
            return {'success' : True, 'message':f"Data for tx_id {self.data_file.split('/')[-1].split('.json')[0]} inserted into db."}
        except Exception as e:
            log.critical('Error in '+self.makeRequests.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.readDataFromFile.__name__+f': {e}'}
#     def loadDataToStaging(self, df):
#         try:
#             log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
#             df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
#             log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
#             self.conn.commit()
#             return {'success' : True}
#         except Exception as e:
#             log.critical('Error in '+self.loadDataToStaging.__name__+f': {e}')
#             return {'success' : False, 'message' : 'Error in '+self.loadDataToStaging.__name__+f': {e}'}
    
    def processDCS(self):
        try:
            log.info(f"Processing file {self.data_file[16:]}")
            read_from_file_resp = self.readDataFromFile()
            if not read_from_file_resp['success']:
                return {'success': False, 'message' : read_from_file_resp['message']}
            data = read_from_file_resp['data']
            make_req_resp = self.makeRequests(data)          
            if make_req_resp['success']:             
                return {'success' : True}
            else:
                return {'success':False, 'message': make_req_resp['message']}
        except Exception as e:
            log.critical('Error in '+self.processDCS.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.processIEGData.__name__+f': {e}'}
                 
# def pipeline_handler(connection,connection_type):
    # log_file = f"iegETL{connection_type}etl.log"
    # logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
    #     format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    # connect_to_db_resp = connection()
    # if connect_to_db_resp['success']: 
    #     engine = connect_to_db_resp['engine']
    #     conn = connect_to_db_resp['conn']
    #     cur = connect_to_db_resp['cur']
    # else:
    #     return {'success' : False, 'message' : connect_to_db_resp['message']}
    
    # log.info(f"IEG {connection_type} ETL job started at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
    # resp = iegETL(engine,conn,cur).processIEGData()

    # if connection_type in ('Production','Dev'):  
    #     file_resp=fileUploadUtils().updateFileStatusInDB(resp['success'],log_file,file_name,cur,conn)
    #     if file_resp["success"]:
    #         log.info("status update success")
    #     else :
    #         log.debug(f"Error while updateing status {file_resp['message']}")

    # if connection_type != 'Production':  
    #     move_resp=fileUploadUtils().moveProcessedFile(resp['success'], file_name, str(datetime.fromtimestamp(os.path.getctime(file_name))))
    #     if move_resp["success"]:
    #         log.info("File move success")
    #     else :
    #         log.debug(f"Error while moving file {move_resp['message']}")
    
    # if resp['success']:
    #     log.info(f"{connection_type} IEG ETL JOB Ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
    #     subject = f'INFO : {connection_type} IEG ETL job status'
    #     body = f'{connection_type} IEG ETL job succeeded. Please check the attached logs for details.'

    # else:
    #     log.critical(f"{connection_type} IEG ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
    #     subject = f'CRITICAL :{connection_type} IEG ETL job status'    
    #     body = f"{connection_type} IEG ETL job failed. Error : {resp['message']} Please check the attached logs for details."

    # total_time = time.time() - start_time
    # log.info(f"Execution time of  IEG{connection_type}ETL is {str(total_time/60)} Minutes")
    # emailClient().send_mail(subject, body, log_file)
    # log.handlers.clear()
    # os.remove(log_file)
    # conn.close()

def pipeline_handler():
    log_file = f"DCSETL.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
    format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    connect_to_db_resp = dbClients('STAGING').connectToDb()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    dcsTriggeredETL(engine, conn, cur).processDCS()
    
    
if __name__ == '__main__':
    pipeline_handler()