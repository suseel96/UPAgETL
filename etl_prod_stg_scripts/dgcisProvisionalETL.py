# ----------------------------------------------------------------

#                 DGCIS Provisional File Upload ETL JOB

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

class dgcisProvisionalETL:

    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = "staging"
        self.DB_STAGING_TABLE_NAME = "stgDGCISProvisional"

    @commonUtills.handleException(log)
    def __readDataFromFile(self):
        log.info(f'Reading data from {self.data_file[16:]}')
        if self.data_file.split('.')[-1] == 'xlsx':
            df = pd.read_excel(self.data_file,header=None)
        elif self.data_file.split('.')[-1] == 'csv':
            df = pd.read_csv(self.data_file, sep=",", encoding='cp1252', skipinitialspace=True)
        log.info(f'Data read from {self.data_file[16:]} successsfully.')
        return {'success' : True, 'data' : df}
    
    @commonUtills.handleException(log)
    def __validateFormat(self,df):
        log.info("Validating file format")
        validation1=['trade type', 'current year', 'from date', 'to date']
        validation2=["trade quantity","trade value"]
        validation3=['item description','itchs','previous year','previous year upto current date','current year upto current date',\
    'previous year previous month','current year previous month','previous year current month upto current date',\
    'current year current month upto current date']
        validation4=['qty', 'value']
        v1_res=all([True if y in [str(x).strip(":").lower() for x in df.iloc[0] if not pd.isna(x)][::2] else False  for y in validation1 ])
        v2_data=[x.lower() for x in df.loc[2] if not pd.isna(x)]
        v2_res=all([True if j in v2_data[0] or v2_data[1] else False for j in validation2])
        v3_res=all([ True if y in   [x.strip().lower() for x  in df.iloc[3] if not pd.isna(x)] else False for y in validation3])
        v4_res=all([True if y in [x.strip().lower() for x in df.iloc[5].unique() if not pd.isna(x)] else False for y in validation4])

        if not all((v1_res,v2_res,v3_res,v4_res)):
            log.critical("File format validation failed.")
            return {"success":False,"message":"File formate validation failed."}
        log.info("File format validated")
        return {"success":True}

    @commonUtills.handleException(log)
    def __transformData(self,df,file_upload_timestamp):
        log.info("Transforming data")
        type_,current_year,from_date,to_date=[x for x in df.iloc[0] if not pd.isna(x)][::-2][::-1]
        quantity_unit,value_unit=[x.split("in")[-1].strip() if "quantity" in x.lower() else x.split("in")[-1].strip() \
                                for x in df.loc[2] if not pd.isna(x)]
        df.columns=df.iloc[3:6].apply(lambda x:x.ffill(),axis=1).values.tolist()
        df.columns=["_".join(sub_col.strip() for sub_col in col if not pd.isna(sub_col)) for col in df.columns]
        df.columns=[f"{col}_{quantity_unit}" if "qty" in col.lower() else  f"{col}_{value_unit}" if "value" in col.lower()\
                     else col for col in df.columns]
        df=df.iloc[6:].reset_index(drop=True)
        
        max_index=df["Item Description"][df.iloc[:,0].isna()].index[0]
        df=df.iloc[:max_index]
        subset=[x for x in df.columns if x.lower() not in ('item description', 'itchs') ]
        df.dropna(inplace=True,subset=subset)
        df=df.melt(id_vars=["Item Description","ITCHS"],var_name="qty_value")
        df["Metric"]=df.qty_value.apply(lambda text:text.split("_")[0].title())
        df["Time Period"]=df.qty_value.apply(lambda text:text.split("_")[1])
        df["UOM"]=df["qty_value"].apply(lambda x:x.split("_")[-1])
        df["qty_value"]=df["qty_value"].apply(lambda x:x.split("_")[-2])
        df["qty_value"] = df["qty_value"].str.title()
        df["Trade Type"]=type_
        df["Current Year"]=current_year
        df["From Date"]=from_date.date()
        df["To Date"]=to_date.date()
        df["file_upload_timestamp"]=file_upload_timestamp
        df["file_name"]=self.data_file.split('/')[-1]
        log.info("Data transformed")
        return {"success":True,"data":df}
    
    @commonUtills.handleException(log)
    def __loadDataToStaging(self, df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        self.conn.commit()
        return {'success' : True}

    def _processDgcisProvisionalData(self):
        file_ctime = os.path.getctime(self.data_file)
        upload_timestamp = str(datetime.fromtimestamp(file_ctime))

        read_from_file_resp=self.__readDataFromFile()
        if not read_from_file_resp["success"]:
            return {"success":False,"message":read_from_file_resp["message"]}

        df_raw=read_from_file_resp["data"]

        validate_resp=self.__validateFormat(df_raw)
        if not validate_resp["success"]:
            return {"success":False,"message":validate_resp["message"]}
        
        transform_data_resp=self.__transformData(df_raw,upload_timestamp)
        if not transform_data_resp["success"]:
            return {"success":False,"message":transform_data_resp["message"]}
        
        transform_data=transform_data_resp["data"]
        format_column_resp=commonUtills().formatColumnNames(df=transform_data)
        if not format_column_resp["success"]:
            return {"success":False,"message":format_column_resp["message"]}
        final_df=format_column_resp["data"]

        load_to_staging_resp=self.__loadDataToStaging(final_df)

        if not load_to_staging_resp["success"]:
            return {"success":False,"message":load_to_staging_resp["message"]}
        return {"success":True}
    
def pipeline_handler(connection,connection_type):
    log_file = f"dgcisProvisional{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        log.critical(connect_to_db_resp["message"])
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"dgcisProvisional {connection_type} ETL job started at {start_time_stamp}")
    resp = dgcisProvisionalETL(engine,conn,cur)._processDgcisProvisionalData()
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
        log.info(f"dgcisProvisional ETL job ended at {end_timestamp}")
        subject = f'INFO : dgcisProvisional ETL job status'
        body = f'dgcisProvisional ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"dgcisProvisional ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : dgcisProvisional ETL job status'
        body = f"dgcisProvisional ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of dynamicdatatriangulation V4 {(connection_type).lower()}etl is {round(total_time,3)} Minutes")
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
        

