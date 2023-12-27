# ----------------------------------------------------------------

#   Mahalanobis National Crop Forecast Centre File Upload ETL JOB

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

class mncfcETL:
    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgMNCFCData'

    @commonUtills.handleException(log)
    def readSheetNames(self):
        log.info(f'Reading sheet names for {self.data_file[16:]}')
        sheet_names = pd.ExcelFile(self.data_file).sheet_names
        return {'success' : True, 'sheetNames' : sheet_names}
    
    @commonUtills.handleException(log)
    def validate_format(self,sheet_names):
        log.info("Starting Validating File Format")
        for sheet in sheet_names:
            columns=pd.read_excel(self.data_file,header=[3,4,5,6]).columns
            v1=('cropyear:', 'month:', 'area uom:', 'yield uom:', 'production uom:','as on date:')
            v2=('state lgd code', 'distt lgd code', 'location name', 'location', 'f1','f2','f3')
            v3=('kharif', 'rabi')
            v4=('area', 'yield', 'production', 'remarks')
            v1_res=[True if x in v1 else False for x in columns.get_level_values(0)[::2].str.lower()]
            v2_res=[True if x in v2 else False for x in columns.get_level_values(1).str.lower().unique() if "unnamed" not in x]
            v3_res=[True if x in v3 else False for x in columns.get_level_values(2).str.lower() if "unnamed" not in x]
            v4_res=[True if x in v4 else False for x in columns.get_level_values(3).str.lower().unique() if "unnamed" not in x]
            if not all((v1_res,v2_res,v3_res,v4_res)):
                log.critical("File format validation failed.")
                return {"success":False,"message":"File format validation failed."}
            else:
                log.info(f'Format validation was successful for {sheet}')
        return {"success":True}

    @commonUtills.handleException(log)
    def readDataFromFile(self, sheet_name):
        log.info(f'Reading data for {sheet_name} from {self.data_file[16:]}')
        if self.data_file.split('.')[-1] == 'xlsx':
            df = pd.read_excel(self.data_file, sheet_name=sheet_name, header=[3,4,5,6])
        elif self.data_file.split('.')[-1] == 'csv':
            df = pd.read_csv(self.data_file, header=[3,4,5,6], sep=",", encoding='cp1252', skipinitialspace=True)
        log.info(f'Data read for {sheet_name} from {self.data_file[16:]} successfully.')
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def transformData(self, df):
        log.info('Transforming data.')
        as_on_date,p_uom,y_uom,a_uom,month,crop_year=df.columns.get_level_values(0)[::-1][::2]
        df.columns=df.columns.droplevel(0)
        df.set_index([df.columns[0],df.columns[1],df.columns[2],df.columns[3]],inplace=True)
        df.columns=df.columns.from_frame(df.columns.to_frame(index=False).applymap(lambda x: np.nan if "Unnamed" in x else x).ffill(axis=0))
        df.index.names=[[j for j in x if "Unnamed" not in j][0] for x in df.index.names]
        df=df.stack(level=[0,1], dropna=False).reset_index()
        df.rename(columns={0:"Forecast",1:"Season"},inplace=True)
        df["Calendar Date Key"] = crop_year[:4]+'0701'
        df['Crop Area UOM'] = a_uom.split()[-1]
        df['Crop Area Scalling'] = 1000
        df['Crop Yield UOM'] = y_uom
        df['Crop Yield Scalling'] = 1
        df['Crop Production Scalling'] = 1000
        df['Crop Production UOM'] = p_uom.split()[-1]
        df["as_on_date"]=as_on_date
        df["Month"]=month
        df.dropna(subset=["Area","Yield","Production"],inplace=True)
        log.info('Data transformations successful.')
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def addAdditionalCols(self, df, file_upload_timestamp):
        log.info('Adding additional columns required.')
        df['District Name'] = df.apply(lambda x: 'All' if x['Location'] == 'State' else
                            'All India' if x['Location'] == 'All India'
                            else x['Location name'], axis=1)
        df['Client Key']='001'
        df['Company Key'] = 'MOA'
        df['Request ID'] = np.nan
        df['User Agency Key'] = 'MNCFC'
        df['Fiscal Year Variant Key']='C2'
        df.rename(columns={'Distt LGD CODE': 'District LGD CODE', 'Location name' : "State Name", 'Location':'Location Type',        
                'Crop' :'Crop Key', 'Area' : "Crop Area",'Production' : "Crop Production",'Yield' : "Crop Yield"
                }, inplace=True)
        df.loc[df['Location Type'] == 'Distt', 'Location Type'] = 'District'
        df['file_upload_timestamp'] = file_upload_timestamp
        df['file_name'] = self.data_file.split('/')[-1]
        df['State Name'] = np.where(df['Location Type'].isin(['State', 'All India']), df['State Name'], np.nan)
        df['State Name'].ffill(inplace=True)
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def standardizeStateNames(self, df):
        log.info('Standardizing state names. Reading state names master  from db.')
        master = pd.read_sql('''SELECT "StateName","Synonyms" FROM staging.state_name_code_lookup
                                WHERE "Source" = 'LGD' ''', self.conn)
        master_list = master.to_dict('records')
        def master_lookup(x):
            for state in master_list:
                if x.lower().strip() in state['Synonyms'].lower():
                    return state['StateName'].upper()
        df['State Name'] = df.apply(lambda x: master_lookup(x['State Name']) if x['Location Type'] == 'State' else x['State Name'], axis=1)
        if df['State Name'].isnull().sum(axis=0) > 0:
            log.info('State names not standardized.')
            return {'success':False, 'message' : 'State names not standardized'}
        else:
            log.info('Standardizing state names successful.')
            return {'success':True, 'data' : df}
    
    @commonUtills.handleException(log)
    def loadDataToStaging(self, df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        self.conn.commit()
        return {'success' : True}

    @commonUtills.handleException(log)
    def loadDataIntoDataTriangulation(self):
        log.info('Loading data into data triangulation.')
        self.cur.execute(''' CALL upag.sp_mncfc(); ''')
        self.conn.commit()
        log.info(f'''Data loaded into data triangulation. ''')
        return {'success':True}

    @commonUtills.handleException(log)
    def processMNCFCData(self):
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
        file_df = pd.DataFrame()
        for sheet in sheet_names:
            read_from_file_resp = self.readDataFromFile(sheet)
            if read_from_file_resp['success']:
                df = read_from_file_resp['data']
            else:
                return {'success': False, 'message' : read_from_file_resp['message']}
            transform_data_resp = self.transformData(df)
            if not transform_data_resp['success']:
                return {'success': False, 'message' : transform_data_resp['message']}
            transformed_df = transform_data_resp['data']
            transformed_df['Crop'] = sheet
            file_df = pd.concat([file_df, transformed_df], ignore_index=True)

        add_additional_cols_resp = self.addAdditionalCols(file_df, upload_timestamp)
        if add_additional_cols_resp['success']:
            final_df = add_additional_cols_resp['data']
            
        else:
            return {'success': False, 'message' : add_additional_cols_resp['message']}
        
        standardize_states_resp = self.standardizeStateNames(final_df)
        if not standardize_states_resp['success']:
            return {'success' : False, 'message' : standardize_states_resp['message']}
        
        transformed_data = standardize_states_resp['data']
    
        load_data_to_staging_resp = self.loadDataToStaging(transformed_data)
        if not load_data_to_staging_resp['success']:
            return {'success': False, 'message' : load_data_to_staging_resp['message']}
    
        triangulation_resp = self.loadDataIntoDataTriangulation()
        if not triangulation_resp['success']:
            return {'success':False,'message':triangulation_resp['message']}
    
        return {"success":True}
        
def pipeline_handler(connection,connection_type):
    log_file = f"mncfc{connection_type}etl.log"
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
    
     
    log.info(f"MNCFC {connection_type} ETL job started at {start_time_stamp}")
    resp = mncfcETL(engine,conn,cur).processMNCFCData()
    if connection_type in ('Production','Dev'):  
        file_resp=fileUploadUtils().updateFileStatusInDB(resp['success'],log_file,file_name,cur,conn)
        if file_resp["success"]:
            log.info("status update success")
        else :
            log.debug(f"Error while updateing status {file_resp['message']}")
            
    if connection_type != 'Production':     
        move_resp=fileUploadUtils().moveProcessedFile(resp['success'], file_name, str(datetime.fromtimestamp(os.path.getctime(file_name))))
        if move_resp["success"]:
            log.info(f"File moved to {'Processed' if resp['success'] else 'Failed'} folder.")
        else :
            log.debug(f"Error while moving file {move_resp['message']}")
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))

    if resp['success']:
        log.info(f"MNCFC {connection_type} ETL job ended at {end_timestamp}")
        subject = f'INFO : MNCFC {connection_type} ETL job status'
        body = f'MNCFC {connection_type} ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"MNCFC {connection_type} ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : MNCFC {connection_type} ETL job status'
        body = f"MNCFC {connection_type} ETL job failed. Error : {resp['message']} Please check the attached logs for details."
        
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of  MNCFC{connection_type}ETL is {round(total_time,3)} Minutes")
    emailClient().send_mail(subject, body, log_file)
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    start_time = time.time()
    pipeline_handler(dbClients(client).connectToDb,client.title())
# start_time = time.time()
# pipeline_handler(dbClients("DEV").connectToDb,"DEV".title())