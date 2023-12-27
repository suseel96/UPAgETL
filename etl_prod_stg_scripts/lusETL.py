# ----------------------------------------------------------------

#   LUS File Upload ETL JOB

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
import regex as re


# log CONFIGURATION
log = logging.getLogger()

file_name = sys.argv[1]

class LUSETL:
    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stglusraw'

    @commonUtills.handleException(log)
    def readSheetNames(self):
        log.info(f'Reading sheet names for {self.data_file[16:]}')
        sheet_names = pd.ExcelFile(self.data_file).sheet_names
        return {'success' : True, 'sheetNames' : sheet_names}
    
    @commonUtills.handleException(log)
    def validate_format(self,sheet_names):
        log.info("Starting Validating File Format")
        for sheet in sheet_names:
            columns= pd.read_excel(self.data_file, sheet_name=sheet,header=[1,2]).columns
            static_cols=all([col.lower() in ('state code','state','area','irrigated area','unirrigated area') for col in columns.get_level_values(1)])
            if not static_cols:
                log.critical(f"Format Validation for {sheet} Failed")
                return {"success":False,"message":f"file format validation failed for {self.data_file[16:]} and sheet {sheet}"}
        log.info(f'Format validation was successfull.')
        return {"success":True}
    
    @commonUtills.handleException(log)    
    def readDataFromFile(self, sheet_name):
        log.info(f'Reading Data for {sheet_name} from {self.data_file[16:]}.')
        if self.data_file.split('.')[-1] == 'xlsx':
            df = pd.read_excel(self.data_file, header=[1,2],sheet_name=sheet_name)
        log.info(f'Data read for {sheet_name} from {self.data_file[16:]} successfully.')
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def transformData(self,df,sheet):
        as_on_date,crop_year,uom=df.columns.get_level_values(0)[::-2]
        df.columns=df.columns.droplevel(0)
        df['year'] = crop_year
        df['area_uom'] = uom.split(" ")[-1]
        df.dropna(subset=["Area","Irrigated Area","Unirrigated Area"],inplace=True)
        df['as_on_date'] = as_on_date
        df['crop_key'] = sheet
        df.rename(columns={"State":"state_name","State Code":"state_code","Area":"crop_area-acerage"
                           ,"Irrigated Area":"crop_irrigated_area-acerage","Unirrigated Area":"crop_unirrigated_area-acerage"},inplace=True)
        return {"success":True,"data":df}
    
    @commonUtills.handleException(log)
    def addAdditionalCols(self,df,file_upload_timestamp):
        log.info('Adding additional columns required.')
        df['company_key'] = 'MOA'
        df['request_id'] = np.nan
        df["client_key"]="001"
        df['user_agency_key']='LUS'
        df['fiscal_year_variant_key'] = 'C2'
        df['calendar_date_key'] = df['year'].apply(lambda x:f"{x.split('-')[0]}0701")
        df['season'] = 'Total'
        df['area_scaling'] = 1000
        df['file_upload_timestamp'] = file_upload_timestamp
        df['file_name'] = self.data_file.split('/')[-1]
        log.info("Additional columns added.")
        return {'success' : True, 'data' : df}

    @commonUtills.handleException(log)
    def loadDataToStaging(self, df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        self.conn.commit()
        return {'success' : True}
    
    @commonUtills.handleException(log)
    def loadDataToLUSV1(self):
        log.info(f'Loading latest data into staging.stglusv1')
        self.cur.execute( """
            select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'LUS' and "SeqNo" = 1 """)
        querytext= self.cur.fetchall()[0][0]
        self.cur.execute(f'''{querytext}''')
        self.conn.commit() 
        log.info(f'Latest data loaded into staging.stglusv1')
        self.conn.commit()
        return {'success' : True}
    
    @commonUtills.handleException(log)
    def processLusData(self):
        log.info(f"Processing file {self.data_file[16:]}")   
        file_ctime = os.path.getctime(self.data_file)
        upload_timestamp = str(datetime.fromtimestamp(file_ctime))
        read_sheet_names_resp = self.readSheetNames()
    
        if not read_sheet_names_resp['success']:
            return {'success': False, 'message' : read_sheet_names_resp['message']}
        
        sheet_names = read_sheet_names_resp['sheetNames']
        validation_resp=self.validate_format(sheet_names)
        if not validation_resp["success"]:
            return {'success': False, 'message' : validation_resp['message']}
        
        file_df=pd.DataFrame()
        for sheet_name in sheet_names:
            read_from_file_resp = self.readDataFromFile(sheet_name)
            if not read_from_file_resp['success']:
                return {'success': False, 'message' : read_from_file_resp['message']}
            df = read_from_file_resp['data']

            transform_data_resp=self.transformData(df,sheet_name)
            if not transform_data_resp['success']:
                return {'success': False, 'message' : transform_data_resp['message']}
            transformed_df = transform_data_resp['data']
            file_df = pd.concat([file_df,transformed_df],ignore_index=True)
        add_col_resp=self.addAdditionalCols(file_df,upload_timestamp)
        if not add_col_resp['success']:
            return {'success': False, 'message' : add_col_resp['message']}
        final_df = add_col_resp['data']
        load_data_to_staging_resp = self.loadDataToStaging(final_df)
        if not load_data_to_staging_resp['success']:
            return {'success': False, 'message' : load_data_to_staging_resp['message']}
        load_data_to_lus_resp = self.loadDataToLUSV1()
        if not load_data_to_lus_resp['success']:
            return {'success': False, 'message' : load_data_to_lus_resp['message']}
        return {"success":True}
                 
def pipeline_handler(connection,connection_type):
    log_file = f"lus{connection_type}etl.log"
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
    log.info(f"LUS {connection_type} ETL job started at {start_time_stamp}")
    resp = LUSETL(engine,conn,cur).processLusData()
    
    if connection_type in ('Staging','Dev'):  
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
        log.info(f"LUS {connection_type} ETL job ended at {end_timestamp}")
        subject = f'INFO : LUS {connection_type} ETL job status'
        body = f'LUS {connection_type} ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"LUS {connection_type} ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : LUS {connection_type} ETL job status'
        body = f"LUS {connection_type} ETL job failed. Error : {resp['message']} Please check the attached logs for details."
        
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60

    log.info(f"Execution time of  LUS{connection_type}ETL is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)

    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())
# pipeline_handler(dbClients("DEV").connectToDb,"DEV".title())