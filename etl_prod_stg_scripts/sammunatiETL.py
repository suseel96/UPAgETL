# ----------------------------------------------------------------

#                 Sammunati File Upload ETL JOB

# -----------------------------------------------------------------
#Imports
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

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

class sammunatiETL:
    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgSammunatiData'
    
    @commonUtills.handleException(log)
    def __readSheetNames(self):
        log.info(f'Reading sheet names for {self.data_file[16:]}')
        sheet_names = pd.ExcelFile(self.data_file).sheet_names
        return {'success' : True, 'sheetNames' : sheet_names}
    
    @commonUtills.handleException(log)
    def __validate_format(self,sheet_names):
        log.info(f"Validating file format for {self.data_file[16:]}")
        for sheet in sheet_names:
            columns = pd.read_excel(self.data_file, sheet_name=sheet, header=[1,2,3]).columns
            v1=('state lgd code', 'crop year:', 'area uom:', 'yield uom:','production uom:', 'month:', 'as on date:')
            v2=('kharif', 'rabi', 'summer')
            v3=('area', 'yield', 'production', 'remarks')
            v1_res=all([True if x in v1 else False for x in columns.get_level_values(0)[::2].str.lower()])
            v2_res=all([True if x in v2 else False for x in columns.get_level_values(1).str.lower() if "unnamed" not in x])
            v3_res=all([True if x in v3 else False for x in columns.get_level_values(2).unique().str.lower() if "unnamed" not in x])
            if not all((v1_res,v2_res,v3_res)):
                log.critical(f"Format Validation for {sheet} Failed")
                return {"success":False,"message":f"file format validation failed for {self.data_file[16:]} and sheet {sheet}"}
        log.info(f"Format validation successfull for {self.data_file[16:]}")
        return {"success":True}

    @commonUtills.handleException(log)
    def __readDataFromFile(self, sheet_name):
        log.info(f'Reading data for {sheet_name} from {self.data_file[16:]}')
        if self.data_file.split('.')[-1] == 'xlsx':
            df = pd.read_excel(self.data_file, sheet_name=sheet_name, header=[1,2,3])
        elif self.data_file.split('.')[-1] == 'csv':
            df = pd.read_csv(self.data_file, header=[1,2,3], sep=",", encoding='cp1252', skipinitialspace=True)
        log.info(f'Data read for {sheet_name} from {self.data_file[16:]} successfully.')
        return {'success' : True, 'data' : df}
        
    @commonUtills.handleException(log)
    def __transform(self,df,sheet,file_upload_timestamp):
        log.info("Transforming data")
        df.set_index([df.columns[0],df.columns[1]],inplace=True)
        df.index.names=[[y for y in x if "Unnamed" not in y][0] for x in df.index.names]
        as_on_date,month,p_uom,y_uom,a_uom,crop_year=df.columns.get_level_values(0)[::-1][::2]
        df.columns=df.columns.droplevel(0)
        df.columns=df.columns.from_frame(df.columns.to_frame(index=False).applymap(lambda x: np.nan if "Unnamed" in str(x) else x).ffill(axis=0))
        df=df.stack(level=[0],dropna=False).reset_index()
        df.dropna(subset=["Area","Production","Yield","Remarks"],inplace=True)
        df.rename(columns={"State name":"State",0:"Season","Area":"Crop Area - Acreage","Production":"Crop Production","Yield":"Crop Yield"},inplace=True)
        df["Fiscal Year Variant Key"]="C2"
        df["Calendar Date Key"]=crop_year
        df["Month"]=month
        df["Crop Area UOM - Hectares"]=a_uom.split()[-1]
        df["Crop Area Scaling"]=1000
        df["Crop Production UOM"]=p_uom.split()[-1]
        df["Crop Production Scaling"]=1000
        df["Crop Yield UOM"]=y_uom
        df["Crop Yield Scaling"]=1
        df["Crop Key"]=sheet
        df["Client Key"]=None
        df["Company Key"]=None
        df["Request ID"]=None
        df["User Agency Key"]="MOA"
        df["file_upload_timestamp"]=file_upload_timestamp
        df["file_name"]=self.data_file.split('/')[-1]
        df["as_on_date"]=as_on_date
        df=df[["Client Key","Company Key","Request ID","User Agency Key","Fiscal Year Variant Key",
               "Calendar Date Key","State","Crop Key","Season","Crop Area - Acreage","Crop Area UOM - Hectares","Crop Area Scaling",
               "Crop Yield","Crop Yield UOM","Crop Yield Scaling","Crop Production","Crop Production UOM","Crop Production Scaling","Month",
               "Remarks","State LGD code","file_upload_timestamp","file_name","as_on_date"]]
        log.info("Transformed data")
        return {"success":True,"data":df}
    
    @commonUtills.handleException(log)
    def __loadDataToStaging(self, df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        self.conn.commit()
        return {'success' : True}
    
    @commonUtills.handleException(log)
    def _processSammunatiData(self):
        log.info(f"Processing file {self.data_file[16:]}")  
        file_ctime = os.path.getctime(self.data_file)
        upload_timestamp = str(datetime.fromtimestamp(file_ctime))
        read_sheet_names_resp = self.__readSheetNames()
        if not read_sheet_names_resp['success']:
            return {'success': False, 'message' : read_sheet_names_resp['message']}
        sheet_names = read_sheet_names_resp['sheetNames']

        validation_resp=self.__validate_format(sheet_names)
        if not validation_resp["success"]:
            return {'success': False, 'message' : validation_resp['message']}
        
        file_df = pd.DataFrame()
        for sheet in sheet_names:
            read_from_file_resp = self.__readDataFromFile(sheet)
            transform_data_resp=self.__transform(read_from_file_resp["data"],sheet,upload_timestamp)
    
            if not transform_data_resp["success"]:
                return transform_data_resp
            trasformed_data=transform_data_resp["data"]
            file_df=pd.concat([file_df,trasformed_data],ignore_index=True)
  
        load_to_staging_resp=self.__loadDataToStaging(file_df)

        if not load_to_staging_resp["success"]:
            return load_to_staging_resp
     
        return {"success":True}

    
def pipeline_handler(connection,connection_type):
    
    log_file = f"sammunati{connection_type}etl.log"
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
    log.info(f"sammunati {connection_type} ETL job started at {start_time_stamp}")
    resp = sammunatiETL(engine,conn,cur)._processSammunatiData()
    
    if connection_type == 'Production':  
        file_resp=fileUploadUtils().updateFileStatusInDB(resp['success'],log_file,file_name,cur,conn)
        if file_resp["success"]:
            log.info("status update success")
        else :
            log.critical(f"Error while updating status {file_resp['message']}")
    if connection_type != 'Production':   
        move_resp=fileUploadUtils().moveProcessedFile(resp['success'], file_name, str(datetime.fromtimestamp(os.path.getctime(file_name))))
        if move_resp["success"]:
            log.info(f"File moved to {'Processed' if resp['success'] else 'Failed'} folder.")
        else :
            log.critical(f"Error while moving file {move_resp['message']}")

    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))

    if resp['success']:
        log.info(f"sammunati {connection_type} ETL job ended at {end_timestamp}")
        subject = f'INFO : sammunati {connection_type} ETL job status'
        body = f'sammunati {connection_type} ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"sammunati {connection_type} ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : sammunati {connection_type} ETL job status'
        body = f"sammunati {connection_type} ETL job failed. Error : {resp['message']} Please check the attached logs for details."
        
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60

    log.info(f"Execution time of  sammunati{connection_type}ETL is {round(total_time,3)} Minutes")
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