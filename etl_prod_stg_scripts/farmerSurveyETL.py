# ----------------------------------------------------------------

#                 Farmer Survey File Upload ETL JOB

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

class farmerSurveyETL:
    def __init__(self,engine,conn,cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = "staging"
        self.DB_STAGING_TABLE_NAME = "stgfarmersurvey"
        self.DB_REPORTING_SCHEMA = "reporting"
        self.DB_REPORTING_TABLE_NAME = ""
        self.DB_REPORTING_MIP_TABLE_NAME = ""  
    
    @commonUtills.handleException(log)
    def __readSheetNames(self):
        log.info(f'Reading sheet names for {self.data_file[16:]}')
        sheet_names = pd.ExcelFile(self.data_file).sheet_names
        return {'success' : True, 'sheetNames' : sheet_names}

    @commonUtills.handleException(log)
    def __readDataFromFile(self, sheet_name):
        log.info(f'Reading data for {sheet_name} from {self.data_file[16:]}')
        if self.data_file.split('.')[-1] == 'xlsx':
            df = pd.read_excel(self.data_file, sheet_name=sheet_name, header=[0,1,2,3])
        elif self.data_file.split('.')[-1] == 'csv':
            df = pd.read_csv(self.data_file, header=[0,1,2,3], sep=",", encoding='cp1252', skipinitialspace=True)
        log.info(f'Data read for {sheet_name} from {self.data_file[16:]} successfully.')
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def __validateFormat(self,sheet_names):
        log.info(f"Validating file format for {self.data_file[16:]}")
        for sheet in sheet_names:
            columns = pd.read_excel(self.data_file, sheet_name=sheet, header=[0,1,2,3]).columns
            validation1=['crop year:', 'kharif', 'rabi']
            validation2=['state lgd code', 'state name', 'yield','farmers perception of yield compared to previous year (absolute value)']
            validation3=['less', 'more ', 'same']
            v1_res=all([True if x.lower() in validation1 else False for x in columns.levels[1][1:]])
            v2_res=all([True if x.lower() in validation2 else False for x in columns.levels[2]])
            v3_res=all([True if x.lower() in validation3 else False for x in columns.levels[3][:3]])
            v4_res="farmers survey yield data" in columns.levels[0][0].lower()
            if not all((v1_res,v2_res,v3_res,v4_res)):
                log.critical("File format validation failed.")
                return {"success":False,"message":"File formate validation failed."}
        log.info(f"Format validation successfull for {self.data_file[16:]}")
        return {"success":True}
    
    @commonUtills.handleException(log)
    def __transformData(self,df,sheet,file_upload_timestamp):
        log.info("Transforming data")
        uom=df.columns.levels[0][0].split(" ")[-1].strip(" ").strip(")")
        df.columns=df.columns.droplevel(0)
        year = df.columns[1][0]
        df = df.set_index([df.columns[0],df.columns[1]])
        df = df.stack(level=[0])
        new_cols = []
        for col in df.columns:
            if type(col) == tuple:
                for sub_col in col:
                    if 'Unname'  not in sub_col and 'farmer' not in sub_col:
                        new_cols.append(sub_col)
        df.columns = new_cols
        df.index.names = ['state_lgd_code','state','season']
        df = df.reset_index()
        df["yield"]=df["Yield"].fillna(0).apply(sum,axis=1)
        df.drop("Yield",axis=1,inplace=True)
        df.dropna(subset=["yield","Same","More","Less"],inplace=True)
        df['year'] = year
        df['uom'] = 'Kg/Ha'
        df['sample_size'] = df['More '] + df['Less'] + df['Same']
        df['crop_name'] = sheet
        df["file_upload_timestamp"]=file_upload_timestamp
        df["file_name"]=self.data_file.split('/')[-1]
        log.info("Transformed data")
        return {'success':True,'data':df}
    
    @commonUtills.handleException(log)
    def __loadDataToStaging(self, df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        self.conn.commit()
        return {'success' : True}
    
    @commonUtills.handleException(log)
    def _startFarmerSurveyETL(self):
        log.info(f"Processing file {self.data_file[16:]}")
        file_ctime = os.path.getctime(self.data_file)
        upload_timestamp = str(datetime.fromtimestamp(file_ctime))
        read_sheets_resp = self.__readSheetNames()
        if not read_sheets_resp['success']:
            return {'success': False, 'message' : read_sheets_resp['message']}
        sheet_names = read_sheets_resp['sheetNames']

        validation_resp=self.__validateFormat(sheet_names)
        if not validation_resp["success"]:
            return {'success': False, 'message' : validation_resp['message']}
        
        file_df = pd.DataFrame()
        for sheet in sheet_names:
            read_from_file_resp = self.__readDataFromFile(sheet)
            transform_data_resp=self.__transformData(read_from_file_resp["data"],sheet,upload_timestamp)
            if not transform_data_resp["success"]:
                return transform_data_resp
            trasformed_data=transform_data_resp["data"]
            file_df=pd.concat([file_df,trasformed_data],ignore_index=True)

        format_column_resp=commonUtills().formatColumnNames(df=file_df)
        if not format_column_resp["success"]:
            return format_column_resp
        final_df=format_column_resp["data"]

        load_to_staging_resp=self.__loadDataToStaging(final_df)

        if not load_to_staging_resp["success"]:
            return load_to_staging_resp

        log.info(f'{self.data_file[16:]} has been processed.')
        return {"success":True}
                 
def pipeline_handler(connection,connection_type):
    log_file = f"farmerSurvey{connection_type.lower()}etl.log"
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
    log.info(f"farmerSurveyETL {connection_type} ETL job started at {start_time_stamp}")
    resp = farmerSurveyETL(engine,conn,cur)._startFarmerSurveyETL()

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
        log.info(f"farmerSurvey {connection_type} ETL job ended at {end_timestamp}")
        subject = f'INFO : farmerSurvey {connection_type} ETL job status'
        body = f'farmerSurvey {connection_type} ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"farmerSurvey {connection_type} ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : farmerSurvey {connection_type} ETL job status'
        body = f"farmerSurvey {connection_type} ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of  farmerSurvey{connection_type}ETL is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())
# start_time = time.time()
# pipeline_handler(dbClients("DEV").connectToDb,"DEV".title())