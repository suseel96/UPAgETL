# ----------------------------------------------------------------

#                 DES MSP File Upload ETL JOB

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

# LOGGING CONFIGURATION
log = logging.getLogger()

file_name = sys.argv[1]

class desMspETL:

    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = "staging"
        self.DB_STAGING_TABLE_NAME = "stgDESMsp"

    def readDataFromFile(self):
        try:
            log.info(f'Reading data from {self.data_file[16:]}')
            if self.data_file.split('.')[-1] == 'xlsx':
                df = pd.read_excel(self.data_file)
            elif self.data_file.split('.')[-1] == 'csv':
                df = pd.read_csv(self.data_file, sep=",", encoding='cp1252', skipinitialspace=True)
            log.info(f'Data read from {self.data_file[16:]} successsfully.')
            return {'success' : True, 'data' : df}
        except Exception as e:
            log.critical('Error in '+self.readDataFromFile.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.readDataFromFile.__name__+f': {e}'}
        
    def transformData(self,df,file_upload_timestamp):
        try:
            log.info("Transforming data")
            year = df.columns[1]
            unit = df.columns[3]
            df.columns = df.iloc[0].str.title()
            df["Crop Year"] = year
            df["UOM"] = unit
            df = df[1:]
            df["Reco Price"] = np.nan
            df.dropna(subset="Msp",inplace=True)
            df = df.rename(columns={"Msp": "Fixed Price"})
            df['file_upload_timestamp'] = file_upload_timestamp 
            df['file_name'] = self.data_file.split('/')[-1]
            log.info("Data tansformed")
            return {"success":True,"data":df}
        
        except Exception as e:
            log.critical('Error in '+self.transformData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.transformData.__name__+f': {e}'}
        
    def validateFormat(self,df):
        try:
            log.info("Validating file format")
            validation1=["msp year","uom"]
            validation2=['crop', 'season', 'msp', 'remarks']
            if all([True if x in df.columns.str.lower().str.strip().str.strip(":") else False for x in validation1]):
                if not all([True if col.lower().strip()  in validation2 else False for col in df.iloc[0]]):
                    log.info("Invalid file columns")
                    return {"success":False,'message':'Error in '+self.validateFormat.__name__+f': {"Invalid file columns"}'}
            else:
                log.info("Invalid file header")
                return {"success":False,'message':'Error in '+self.validateFormat.__name__+f': {"Invalid file header"}'}
            log.info("Format Validated.")
            return {"success":True}
        
        except Exception as e:
            log.critical('Error in '+self.validateFormat.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.validateFormat.__name__+f': {e}'}
        
    def loadDataToStaging(self, df):
        try:
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
            log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToStaging.__name__+f': {e}'}
    
    def loadDataToReporting(self):
        try:
            log.info(f'Loading data into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_TABLE_NAME}')
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'DESMSP' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =1;
            ''')
            log.info(f'Data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_TABLE_NAME}')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
    
    def loadDataToMIP(self):
        try:
            log.info(f'Loading data into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_MIP_TABLE_NAME}')
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'DESMSP' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =2;
            ''')
            log.info(f'Data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_MIP_TABLE_NAME}')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToMIP.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToMIP.__name__+f': {e}'}

    def processDesMspData(self):
        file_ctime = os.path.getctime(self.data_file)
        upload_timestamp = str(datetime.fromtimestamp(file_ctime))

        read_from_file_resp=self.readDataFromFile()
        if not read_from_file_resp["success"]:
            return {"success":False,"message":read_from_file_resp["message"]}

        df_raw=read_from_file_resp["data"]

        validate_resp=self.validateFormat(df_raw)
        if not validate_resp["success"]:
            return {"success":False,"message":validate_resp["message"]}
        
        transform_data_resp=self.transformData(df_raw,upload_timestamp)
        if not transform_data_resp["success"]:
            return {"success":False,"message":transform_data_resp["message"]}
        
        transform_data=transform_data_resp["data"]

        load_to_staging_resp=self.loadDataToStaging(transform_data)

        if not load_to_staging_resp["success"]:
            return {"success":False,"message":load_to_staging_resp["message"]}
        
        # load_to_reporting_resp=self.loadDataToReporting()
        # if not load_to_reporting_resp['success']:
        #     return {'success': False, 'message' : load_to_reporting_resp['message']}
        
        # load_to_mip_resp=self.loadDataToMIP()
        # if not load_to_mip_resp['success']:
        #     return {'success': False, 'message' : load_to_mip_resp['message']}
        return {"success":True}

def pipeline_handler(connection,connection_type):
    log_file = f"desMsp{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"DesMSP ETL job started at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        log.critical(connect_to_db_resp["message"])
        return {'success' : False, 'message' : connect_to_db_resp['message']}

    log.info(f"desMSP {connection_type} ETL job started at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
    resp = desMspETL(engine,conn,cur).processDesMspData()

    if connection_type in ('Production',"Dev"):  
        file_resp=fileUploadUtils().updateFileStatusInDB(resp['success'],log_file,file_name,cur,conn)
        if file_resp["success"]:
            log.info("status update success")
        else :
            log.info(f"Error while updateing status {file_resp['message']}")
    
    if connection_type != 'Production':   
        move_resp=fileUploadUtils().moveProcessedFile(resp['success'], file_name, str(datetime.fromtimestamp(os.path.getctime(file_name))))
        if move_resp["success"]:
            log.info(f"File moved to {'Processed' if resp['success'] else 'Failed'} folder.")
        else :
            log.info(f"Error while moving file {move_resp['message']}")
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"DesMSP ETL job ended at {end_timestamp}")
        subject = f'INFO : DesMSP ETL job status'
        body = f'DesMSP ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"DesMSP ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : DesMSP ETL job status'
        body = f"DesMSP ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of  desMSP{connection_type}ETL is {round(total_time,3)} Minutes")
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