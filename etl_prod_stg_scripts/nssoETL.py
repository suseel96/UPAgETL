# ----------------------------------------------------------------

#                 DGCIS Provisional File Upload ETL JOB

# -----------------------------------------------------------------
#Imports
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import pandas as pd
import pytz
from etl_utils.utils import dbClients,emailClient,fileUploadUtils,commonUtills
from dotenv import load_dotenv
import logging
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# LOGGING CONFIGURATION
log = logging.getLogger()

file_name = sys.argv[1]

class nssoETL:
    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = "staging"
        self.DB_STAGING_TABLE_NAME = "stgNSSO"

    @commonUtills.handleException(log)
    def __readDataFromFile(self):
        log.info(f'Reading data from {self.data_file[16:]}')
        if self.data_file.split('.')[-1] == 'xlsx':
            df = pd.read_excel(self.data_file,header=[1,2])
        elif self.data_file.split('.')[-1] == 'csv':
            df = pd.read_csv(self.data_file, sep=",", encoding='cp1252', skipinitialspace=True,header=[1,2])
        log.info(f'Data read from {self.data_file[16:]} successsfully.')
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def __validateFormat(self,columns):
        log.info("Validating file format")
        v1=('year:', 'season:', 'uom:')
        v2=('year:', '2022-23', 'season:', 'kharif', 'uom:', 'kg/ha')
        v1_res=all([True if x in v1 else False for x in columns.get_level_values(0)[::2].str.lower()])
        v2_res=all([True if x in v2 else False for x in columns.get_level_values(0).str.lower()])
        if not all((v1_res,v2_res)):
            log.critical("File format validation failed.")
            return {"success":False,"message":"File format validation failed."}
        log.info("File format validated")
        return {"success":True}

    @commonUtills.handleException(log)
    def __transformData(self,df,file_upload_timestamp):
        log.info("Transforming data")
        yield_uom,season,year=df.columns.get_level_values(0)[::-1][::2]
        df.columns=df.columns.droplevel(0)
        df=df.melt(id_vars=["State","Crop"],var_name="Sample Type",value_name="Yield")
        df.dropna(subset="Yield",inplace=True)
        df["Year"]=year
        df["Season"]=season.title()
        df["UOM"]='Kg/Ha'
        df["file_upload_timestamp"]=file_upload_timestamp
        df["file_name"]=self.data_file.split('/')[-1]
        log.info("Transformed data")
        return {"success":True,"data":df}

    @commonUtills.handleException(log)
    def __loadDataToStaging(self, df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        self.conn.commit()
        return {'success' : True}
    
    def _processNssoData(self):
        
        file_ctime = os.path.getctime(self.data_file)
        upload_timestamp = str(datetime.fromtimestamp(file_ctime))
        
        read_from_file_resp =self.__readDataFromFile()
        
        if not read_from_file_resp["success"]:
            return read_from_file_resp
        data=read_from_file_resp["data"]

        validate_format_resp=self.__validateFormat(data.columns)

        if not validate_format_resp["success"]:
            return validate_format_resp
        
        tranform_data_resp=self.__transformData(data,upload_timestamp)

        if not tranform_data_resp["success"]:
            return tranform_data_resp
        
        final_df=tranform_data_resp["data"]

        load_to_staging_resp=self.__loadDataToStaging(final_df)

        if not load_to_staging_resp["success"]:
            return load_to_staging_resp
        
        return {"success":True}
    
def pipeline_handler(connection,connection_type):
    log_file = f"nsso{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"Nsso ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        log.critical(connect_to_db_resp["message"])
        return {'success' : False, 'message' : connect_to_db_resp['message']}

    log.info(f"nssoETL {connection_type} ETL job started at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
    resp = nssoETL(engine,conn,cur)._processNssoData()
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
        log.info(f"nsso ETL job ended at {end_timestamp}")
        subject = f'INFO : nsso ETL job status'
        body = f'nsso ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"nsso ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : nsso ETL job status'
        body = f"nsso ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of  nsso{connection_type}ETL is {round(total_time,3)} Minutes")
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
        

