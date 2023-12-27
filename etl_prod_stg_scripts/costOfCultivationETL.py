# ----------------------------------------------------------------

#                 Cost Of Cultivation File Upload ETL JOB

# -----------------------------------------------------------------
#Imports

import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import pytz
from etl_utils.utils import dbClients,emailClient,fileUploadUtils,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import warnings
warnings.filterwarnings('ignore')
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

file_name = sys.argv[1]

class costOfCultivationETL:
    def __init__(self,engine,conn,cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = "staging"
        self.DB_STAGING_TABLE_NAME = "costofcultivation"
        self.DB_REPORTING_SCHEMA = "reporting"
        self.DB_REPORTING_TABLE_NAME = ""
        self.DB_REPORTING_MIP_TABLE_NAME = ""  
    
    @commonUtills.handleException(log)
    def __readDataFromFile(self):
        log.info(f'Reading data from {self.data_file[16:]}')
        if self.data_file.split('.')[-1] == 'xlsx':
            df = pd.read_excel(self.data_file, header=[1,2])
        else :
            return {"success":False,"message":f"Unknown file format {self.data_file.split('.')[-1] }"}
        log.info(f'Data read from {self.data_file[16:]} successfully.')
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def __validateFormat(self,columns):
        log.info(f"Validating file format for {self.data_file[16:]}")
        v1=['crop year', 'as on date']
        v2=['state', 'crop', 'season', 'yield (kg/ha)']
        v1_res=all([True if x in v1 else False for x in columns.get_level_values(0)[::2].str.strip(":").str.lower()])
        v2_res=all([True if x in v2 else False for x in columns.get_level_values(1).str.lower()])
        if not all((v1_res,v2_res)):
            log.critical("File format validation failed.")
            return {"success":False,"message":"File formate validation failed."}
        log.info(f"Format validation successfull for {self.data_file[16:]}")
        return {"success":True}
    
    @commonUtills.handleException(log)
    def __transformData(self, df, file_upload_timestamp):
        log.info('Transforming data.')
        as_on_date,crop_year=df.columns.get_level_values(0)[::-2]
        uom=df.columns[-1][1].split()[1].strip("()")
        df.columns=[x.split()[0].lower() for x in df.columns.get_level_values(1) ]
        df["uom"]=uom
        df["as_on_date"]=as_on_date.strftime("%Y-%m-%d")
        df["crop_year"]=crop_year
        df["file_upload_timestamp"]=file_upload_timestamp
        df["file_name"]=self.data_file.split('/')[-1]
        df.dropna(subset="yield",inplace=True)
        log.info("Data transformed.")
        return {'success':True,'data':df}
    
    @commonUtills.handleException(log)
    def __loadDataToStaging(self, df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        self.conn.commit()
        return {'success' : True}
    
    @commonUtills.handleException(log)
    def _startCostOfCultivationETL(self):
        file_ctime = os.path.getctime(self.data_file)
        upload_timestamp = str(datetime.fromtimestamp(file_ctime))

        read_from_file_resp =self.__readDataFromFile()
        
        if not read_from_file_resp["success"]:
            return read_from_file_resp
        data=read_from_file_resp["data"]

        validate_format_resp=self.__validateFormat(data.columns)

        if not validate_format_resp["success"]:
            return validate_format_resp
        
        transform_data_resp=self.__transformData(data,upload_timestamp)

        if not transform_data_resp["success"]:
            return transform_data_resp
        
        final_df=transform_data_resp["data"]

        format_column_resp=commonUtills().formatColumnNames(df=final_df)
        if not format_column_resp["success"]:
            return format_column_resp
        final_df=format_column_resp["data"]
        
        load_to_staging_resp=self.__loadDataToStaging(final_df)

        if not load_to_staging_resp["success"]:
            return load_to_staging_resp

        return {"success":True}
                 
def pipeline_handler(connection,connection_type):
    log_file = f"costOfCultivation{connection_type.lower()}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    
    log.info(f"CostOfCultivation {connection_type} ETL job started at {start_time_stamp}")
    resp = costOfCultivationETL(engine,conn,cur)._startCostOfCultivationETL()
    if connection_type in ('Production',"Dev"):  
        file_resp=fileUploadUtils().updateFileStatusInDB(resp['success'],log_file,file_name,cur,conn)
        if file_resp["success"]:
            log.info("status update success")
        else :
            log.critical(f"Error while updateing status {file_resp['message']}")
    if connection_type != 'Production':   
        move_resp=fileUploadUtils().moveProcessedFile(resp['success'], file_name, str(datetime.fromtimestamp(os.path.getctime(file_name))))
        if move_resp["success"]:
            log.info(f"File moved to {'Processed' if resp['success'] else 'Failed'} folder.")
        else :
            log.critical(f"Error while moving file {move_resp['message']}")
    
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"CostOfCultivation {connection_type} ETL job ended at {end_timestamp}")
        subject = f'INFO : CostOfCultivation {connection_type} ETL job status'
        body = f'CostOfCultivation {connection_type} ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"CostOfCultivation {connection_type} ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : CostOfCultivation {connection_type} ETL job status'
        body = f"CostOfCultivation {connection_type} ETL job failed. Error : {resp['message']} Please check the attached logs for details."
        
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of  CostOfCultivation {connection_type}ETL is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['STAGING','PRODUCTION']
for client in DB_CLIENTS:
    start_time = time.time()
    pipeline_handler(dbClients(client).connectToDb,client.title())
# start_time = time.time()
# pipeline_handler(dbClients("DEV").connectToDb,"DEV".title())