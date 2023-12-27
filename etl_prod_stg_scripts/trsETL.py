# ----------------------------------------------------------------

#          TRS File Upload ETL JOB

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
import requests
import warnings
warnings.filterwarnings('ignore')
import time
import regex as re
# LOGGING CONFIGURATION
log = logging.getLogger()


file_name = sys.argv[1]
pd.DataFrame().to_excel('trstest.xlsx')

class trsETL:
    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME="stgtrs"

    def readDataFromFile(self):
        try:
            log.info(f'Reading data from {self.data_file[16:]}')
            df = pd.read_excel(self.data_file, header = None, nrows = 2)
            date  = df[8][0]
            uom = df[10][0]
            df = pd.read_excel(self.data_file, header =(1,2))
            df.set_index([df.columns[0],df.columns[1],df.columns[2],df.columns[3],df.columns[4]], inplace= True)
            df = df.stack(level=0, dropna =False).reset_index()
            df.to_excel('testtrs.xlsx', index = False)
            log.info(f'Data read from {self.data_file[16:]} successsfully.')
            return {'success' : True, 'data' : df}
        except Exception as e:
            log.critical('Error in '+self.readDataFromFile.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.readDataFromFile.__name__+f': {e}'}

    def validate_format(self,columns):
        try :
            log.info(f"Validating file format for {self.data_file[16:]}")
            if "crops" in [x.lower() for x in columns[0]] and any('Area' and "Yield" and "Kharif" and "Rabi" in item for item in columns):
                log.info(f"Format validation successfull for {self.data_file[16:]}")
                return {"success":True}
            else :
                log.critical(f"file format validation failed for {self.data_file[16:]}")
                return {"success":False,"message":f"file format validation failed for {self.data_file[16:]}"}
        except Exception as e:
            log.critical('Error in '+self.validate_format.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.validate_format.__name__+f': {e}'}
        
    def transformData(self, df):
        try:
            log.info('Transforming data.')
            df.set_index(df.columns[0], inplace=True)
            df = df.stack(level=[0,1], dropna=False).reset_index()
            log.info('Data transformations successfull.')
            return {'success' : True, 'data' : df}
        except Exception as e:
            log.critical('Error in '+self.transformData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.transformData.__name__+f': {e}'}
        
    def addAdditionalCols(self, df,file_upload_timestamp):
        try:
            log.info('Adding additional columns required.')

            month,year=(x.split("-")[0] if "-" in x else x for x in re.findall(r"\w{3}_\d{4}-\d{2}",self.data_file)[0].split("_"))
            df.loc[df['Crop Key'] == 'Arhar' , 'Crop Key'] = 'Tur'
            df.loc[df['Crop Key'] == 'Jawar' , 'Crop Key'] = 'Jowar'
            df.loc[df['Crop Key'] == 'Masur (Lentil)' , 'Crop Key'] = 'Lentil'
            df.loc[df['Crop Key'] == 'Rapeseed Mustard' , 'Crop Key'] = 'Rapeseed & Mustard'
            df['Crop Key']=df["Crop Key"].str.strip()
            df["Calendar Date Key"] = year+'0701'
            df['Month'] = pd.to_datetime(month,format="%b").month
            df['Crop Production'] = (df['Crop Area - Acerage']*df['Crop Yield'])/1000
            df['Client Key']='001'
            df['Company Key'] = 'MOA'
            df['Request ID'] = np.nan
            df['User Agency Key'] = 'TRS'
            df['District Name'] = np.nan
            df['Fiscal Year Variant Key']='C2'
            df['Crop Area UOM - Hectares'] = 'hectares'
            df['Crop Scaling - 000 or 00000'] = 1000
            df['Crop yield UOM'] = 'kg/hectare'
            df['Crop Yield Scaling'] = 1
            df['Crop Production Scaling'] = 1000
            df['Crop Production UOM'] = 'tonnes'
            df['file_upload_timestamp'] = file_upload_timestamp 
            df['file_name'] = self.data_file.split('/')[-1]
            log.info("Additional columns added.")
            return {'success' : True, 'data' : df}
        except Exception as e:
            log.critical('Error in '+self.addAdditionalCols.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.addAdditionalCols.__name__+f': {e}'}
    
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
    
    # def loadDataIntoDataTriangulation(self):
    #     try:
    #         log.info('Loading data into data triangulation.')
    #         self.cur.execute(''' CALL upag.sp_trs(); ''')
    #         self.conn.commit()
    #         log.info(f'''Data loaded into data triangulation. ''')
    #         return {'success':True}
        
        except Exception as e:
            log.critical(f'Unable to load data into data triangulation. Error: {e}')
            return {'success' : False, 'message' : f'Unable to load data to data triangulation. Error: {e}'}
        

    def processTRSData(self):
        try:
            log.info(f"Processing file {self.data_file[16:]}")
            if "trs" in self.data_file.split('/')[-1].split('.')[0].lower():
                log.info(f"File name validation successfull")
            else:
                log.critical(f"Invalid File name {self.data_file.split('/')[-1].split('.')[0]}")
                return {'success': False, 'message' : f"Invalid File name {self.data_file.split('/')[-1].split('.')[0]}"}
            
            file_ctime = os.path.getctime(self.data_file)
            upload_timestamp = str(datetime.fromtimestamp(file_ctime))
            
            read_from_file_resp = self.readDataFromFile()
            if read_from_file_resp['success']:
                df = read_from_file_resp['data']
            else:
                return {'success': False, 'message' : read_from_file_resp['message']}
            # validation_resp=self.validate_format(df.columns)
            # if not validation_resp["success"]:
            #     return {'success': False, 'message' : validation_resp['message']}
    
            # transform_data_resp = self.transformData(df)
            # if transform_data_resp['success']:
            #     transformed_df = transform_data_resp['data']
            # else:
            #     return {'success': False, 'message' : transform_data_resp['message']}
            # add_additional_cols_resp = self.addAdditionalCols(standardized_df, upload_timestamp)
            # if add_additional_cols_resp['success']:
            #     additional_col_df = add_additional_cols_resp['data']
            # else:
            #     return {'success': False, 'message' : add_additional_cols_resp['message']}

            # load_data_to_staging_resp = self.loadDataToStaging(additional_col_df)
            # if not load_data_to_staging_resp['success']:
            #     return {'success': False, 'message' : load_data_to_staging_resp['message']}
            
            # return {'success' : True}
        
        except Exception as e:
            log.critical('Error in '+self.processTRSData.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.processTRSData.__name__+f': {e}'}
                 
def pipeline_handler(connection,connection_type):
    log_file = f"trsETL{connection_type}etl.log"
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
    
    log.info(f"TRS {connection_type} ETL job started at {start_time_stamp}")
    resp = trsETL(engine,conn,cur).processTRSData()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))

    # if connection_type in ('Production','Dev'):  
    #     # file_resp=fileUploadUtils().updateFileStatusInDB(resp['success'],log_file,file_name,cur,conn)
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
    
    if resp['success']:
        log.info(f"{connection_type} TRS ETL JOB Ended at {end_timestamp}")
        subject = f'INFO : {connection_type} TRS ETL job status'
        body = f'{connection_type} TRS ETL job succeeded. Please check the attached logs for details.'

    else:
        log.critical(f"{connection_type} TRS ETL job ended at {end_timestamp}")
        subject = f'CRITICAL :{connection_type} TRS ETL job status'    
        body = f"{connection_type} TRS ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60

    log.info(f"Execution time of  TRS{connection_type}ETL is {total_time} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    # emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())

start_time = time.time()
pipeline_handler(dbClients("DEV").connectToDb,"DEV".title())