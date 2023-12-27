# ----------------------------------------------------------------

#          Institute of Economic Growth File Upload ETL JOB

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
# LOGGING CONFIGURATION
log = logging.getLogger()


file_name = sys.argv[1]

class iegETL:
    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME="stgIEGData"

    @commonUtills.handleException(log)
    def readDataFromFile(self):
        log.info(f'Reading data from {self.data_file[16:]}')
        if self.data_file.split('.')[-1] == 'xlsx':
            df = pd.read_excel(self.data_file, header=[0,1,2,3])
        elif self.data_file.split('.')[-1] == 'csv':
            df = pd.read_csv(self.data_file, header=[0,1,2,3], sep=",", encoding='cp1252', skipinitialspace=True)
        log.info(f'Data read from {self.data_file[16:]} successsfully.')
        return {'success' : True, 'data' : df}

    @commonUtills.handleException(log)
    def validate_format(self,columns):
        log.info("Validating file format")
        v1=('as on date:', 'month:', 'crop year:', 'yield uom:')
        v2=('bajra','barley','castorseed','cotton','gram','groundnut','jowar','jute','lentil ','maize','moong',\
            'rapeseed & mustard','rice','soybean','state','sugarcane','tur','urad','wheat')
        v3=('rabi', 'kharif')
        v4=('yield', 'area')
        v1_res= all([True if x.lower() in v1 else False for x in columns.get_level_values(0)[:10][::-1][::2][:-1]])
        v2_res=all([True if x.lower() in v2 else False for x in set(columns.get_level_values(1)) if "Unnamed" not in x])
        v3_res=all([True if x.lower() else False in v3 for x in set(columns.get_level_values(2)) if "Unnamed" not in x])
        v4_res=all([True if x.lower() in v4 else False for x in set(columns.get_level_values(3)) if "Unnamed" not in x])
        if not all((v1_res,v2_res,v3_res,v4_res)):
                log.critical("File format validation failed.")
                return {"success":False,"message":"File format validation failed."}
        log.info("File format validated")
        return {"success":True}
    
    @commonUtills.handleException(log)
    def transformData(self, df):
        log.info('Transforming data.')
        as_on_date,month,crop_year,yield_uom,area_uom,_=df.columns.get_level_values(0)[:11][::-1][::2]
        month=pd.to_datetime(month,format="%B").month
        df.columns=df.columns.droplevel(0)
        df=df.set_index(df.columns[0])
        df.index.names=[[y for y in x if "Unnamed" not in y][0] for x in df.index.names]
        df.columns=df.columns.from_frame(df.columns.to_frame(index=False).applymap(lambda x: np.nan if "Unnamed" in x else x ).ffill(axis=0))
        df=df.stack(level=[0,1], dropna=False).reset_index()
        df.dropna(subset=["Area","Yield"],inplace=True)
        df.rename(columns={"State":"State name",0:"Crop Key",1:"Season","Area":"Crop Area - Acerage","Yield":"Crop Yield"},inplace=True)
        df["as_on_date"]=as_on_date.strftime("%Y-%m-%d")
        df['Month'] = month
        df['Crop Production'] = (df['Crop Area - Acerage']*df['Crop Yield'])/1000
        df['Crop Area UOM - Hectares'] = area_uom.split()[-1].lower()
        df['Crop Scaling - 000 or 00000'] = 1000
        df['Crop yield UOM'] = yield_uom.lower()
        df['Crop Yield Scaling'] = 1
        df['Crop Production Scaling'] = 1000
        df['Crop Production UOM'] = 'tonnes'
        df["Calendar Date Key"] = crop_year[:4]+'0701'
        df["YearMonthCode"]=crop_year[:4]+str(month) if month>9 else "0"+str(month)
        log.info('Data transformations successfull.')
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def standardizeStateNames(self,df):
        log.info('Standardizing state names. Reading state names master  from db.')
        master = pd.read_sql('''SELECT "StateName","Synonyms" FROM staging.state_name_code_lookup
                                WHERE "Source" = 'LGD' ''',self.conn)
        master_list = master.to_dict('records')
        def master_lookup(x):
            for state in master_list:
                if x.lower() in state['Synonyms'].lower():
                    return state['StateName']
        
        df['State name'] = df['State name'].apply(master_lookup)
        if df['State name'].isnull().sum(axis=0) > 0:
            log.critical('State names not standardized.')
            return {'success':False, 'message' : 'State names not standardized'}
        log.info('Standardizing state names successful.')
        return {'success':True, 'data' : df}
    
    @commonUtills.handleException(log)
    def addAdditionalCols(self, df,file_upload_timestamp):
        log.info('Adding additional columns required.')
        df.loc[df['Crop Key'] == 'Arhar' , 'Crop Key'] = 'Tur'
        df.loc[df['Crop Key'] == 'Jawar' , 'Crop Key'] = 'Jowar'
        df.loc[df['Crop Key'] == 'Masur (Lentil)' , 'Crop Key'] = 'Lentil'
        df.loc[df['Crop Key'] == 'Rapeseed Mustard' , 'Crop Key'] = 'Rapeseed & Mustard'
        df['Crop Key']=df["Crop Key"].str.strip()
        df['Client Key']='001'
        df['Company Key'] = 'MOA'
        df['Request ID'] = np.nan
        df['User Agency Key'] = 'IEG'
        df['District Name'] = np.nan
        df['Fiscal Year Variant Key']='C2'
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
    def loadDataIntoDataTriangulation(self):
        log.info('Loading data into data triangulation.')
        self.cur.execute(''' CALL upag.sp_ieg(); ''')
        self.conn.commit()
        log.info(f'''Data loaded into data triangulation. ''')
        return {'success':True}
        
    @commonUtills.handleException(log)
    def processIEGData(self):
        log.info(f"Processing file {self.data_file[16:]}")    
        file_ctime = os.path.getctime(self.data_file)
        upload_timestamp = str(datetime.fromtimestamp(file_ctime))
        read_from_file_resp = self.readDataFromFile()

        if not read_from_file_resp['success']:
            return {'success': False, 'message' : read_from_file_resp['message']}
        
        df = read_from_file_resp['data']
        validation_resp=self.validate_format(df.columns)

        if not validation_resp["success"]:
            return {'success': False, 'message' : validation_resp['message']}

        transform_data_resp = self.transformData(df)

        if not transform_data_resp['success']:
            return {'success': False, 'message' : transform_data_resp['message']}
        
        transformed_df = transform_data_resp['data']

        add_additional_cols_resp = self.addAdditionalCols(transformed_df, upload_timestamp)

        if not add_additional_cols_resp['success']:
            return {'success': False, 'message' : add_additional_cols_resp['message']}
        
        additional_col_df = add_additional_cols_resp['data']

        standardized_state_df_resp=self.standardizeStateNames(additional_col_df)

        if not standardized_state_df_resp['success']:
            return {'success': False, 'message' : standardized_state_df_resp['message']}
        
        standardized_state_df=standardized_state_df_resp["data"]
    
        load_data_to_staging_resp = self.loadDataToStaging(standardized_state_df)

        if not load_data_to_staging_resp['success']:
            return {'success': False, 'message' : load_data_to_staging_resp['message']}
        
        load_data_into_data_triangulation_resp = self.loadDataIntoDataTriangulation()

        if not load_data_into_data_triangulation_resp['success']:
            return {'success': False, 'message' : load_data_into_data_triangulation_resp['message']}
        return {'success' : True}
                 
def pipeline_handler(connection,connection_type):
    log_file = f"iegETL{connection_type}etl.log"
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
    
    log.info(f"IEG {connection_type} ETL job started at {start_time_stamp}")
    resp = iegETL(engine,conn,cur).processIEGData()

    if connection_type in ('Production','Dev'):  
        file_resp=fileUploadUtils().updateFileStatusInDB(resp['success'],log_file,file_name,cur,conn)
        if file_resp["success"]:
            log.info("status update success")
        else :
            log.critical(f"Error while updateing status {file_resp['message']}")

    if connection_type != 'Production':  
        move_resp=fileUploadUtils().moveProcessedFile(resp['success'], file_name, str(datetime.fromtimestamp(os.path.getctime(file_name))))
        if move_resp["success"]:
            log.info("File move success")
        else :
            log.critical(f"Error while moving file {move_resp['message']}")
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} IEG ETL JOB Ended at {end_timestamp}")
        subject = f'INFO : {connection_type} IEG ETL job status'
        body = f'{connection_type} IEG ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"{connection_type} IEG ETL job ended at {end_timestamp}")
        subject = f'CRITICAL :{connection_type} IEG ETL job status'    
        body = f"{connection_type} IEG ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of  IEG{connection_type}ETL is {total_time} Minutes")
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