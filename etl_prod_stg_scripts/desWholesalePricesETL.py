# ----------------------------------------------------------------

#                 DES Wholesale File Upload ETL JOB

# -----------------------------------------------------------------
#Imports
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import pandas as pd
import numpy as np
import pytz
from dotenv import load_dotenv
from etl_utils.utils import dbClients,emailClient,fileUploadUtils,commonUtills
import logging
from datetime import timedelta, datetime
import warnings
warnings.filterwarnings('ignore')

# LOGGING CONFIGURATION
log = logging.getLogger()

file_name = sys.argv[1]

class desWholesalePricesETL:
    def __init__(self, engine, conn, cur):
        self.data_file = file_name
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = "staging"
        self.DB_STAGING_TABLE_NAME = "stgDESWholesalePrices"
        self.DB_REPORTING_SCHEMA = "reporting"
        self.DB_REPORTING_TABLE_NAME = "RptDESWholesalePrices"
        self.DB_REPORTING_MIP_TABLE_NAME = "RptMIP"
    
    def readDataFromFile(self):
        try:
            log.info(f'Reading data from {self.data_file[16:]}')
            required_columns=['crop','variety','comm_group','centre_name','state_name','district_name',\
                              'unit','modal_price','date']
            if self.data_file.split('.')[-1] == 'xlsx':
                df = pd.read_excel(self.data_file)
            elif self.data_file.split('.')[-1] == 'csv':
                df = pd.read_csv(self.data_file)
            if not all([True if x in required_columns else False for x in df.columns.str.lower()]):
                log.info('Please check the data , Columns are not matching')
                return {'success':False,'message':'Columns are not matching'}
            df.dropna(subset="Modal_price",inplace=True)
            log.info(f'Data read from {self.data_file[16:]} successsfully.')
            return {'success' : True, 'data' : df}
        except Exception as e:
            log.critical('Error in '+self.readDataFromFile.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.readDataFromFile.__name__+f': {e}'}
        
    
    def validatingDates(self,df):
        try:
            self.cur.execute('''select max("Date") from staging."stgDESWholesalePrices"''')
            max_date = self.cur.fetchall()[0][0]
            df['Date'] = pd.to_datetime(df['Date'],format ='%d-%m-%y')
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            for date in sorted(df['Date'].unique()):
                date=datetime.strptime(date, "%Y-%m-%d").date()
                if max_date+timedelta(days=7)==date:
                    max_date=date
                else:
                    log.critical(f'validation of dates unsuccessful:Please check the data.')
                    return {'success':False, 'message': 'validation of dates unsuccessful:Please check the data.'}
        except Exception as e:
            log.critical('Error in '+self.validatingDates.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.validatingDates.__name__+f': {e}'}
        else:
            log.info(f"Data avaliable for {sorted(df['Date'].unique())}")
            return {'success':True}
        
    def addAdditionalCols(self,df,file_upload_timestamp):
        try:
            df['file_upload_timestamp'] = file_upload_timestamp 
            df['file_name'] = self.data_file.split('/')[-1]
            df = df.replace(r'^\s*$', np.nan, regex=True)
            return {"success":True,"data":df}
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
            
    def loadDataToReporting(self):
        try:
            log.info(f'Loading data into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_TABLE_NAME}')
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'desWholesalePrices' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =1;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text)
            self.conn.commit()
            log.info(f'Data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_TABLE_NAME}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
    
    def loadDataToMIP(self):
        try:
            log.info(f'Loading data into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_MIP_TABLE_NAME}')
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'desWholesalePrices' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =2;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text)
            self.conn.commit()
            log.info(f'Data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_MIP_TABLE_NAME}')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToMIP.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToMIP.__name__+f': {e}'}
        finally:
            self.conn.close()
            log.info('Database connection closed.')
        
    def processDESWholesalePriceData(self):
        try:
            log.info(f"Processing file {self.data_file[16:]}")
            file_ctime = os.path.getctime(self.data_file)
            upload_timestamp = str(datetime.fromtimestamp(file_ctime))
            file_resp = self.readDataFromFile()
            if not file_resp['success']:
                return {'success':False,'message':file_resp['message']}
            df = file_resp['data']

            validation_resp = self.validatingDates(df)
            if not validation_resp['success']:
                return {'success':False,'message':validation_resp['message']}
            
            add_col_resp=self.addAdditionalCols(df,upload_timestamp)
            if not add_col_resp["success"]:
                return {'success':False,'message':add_col_resp['message']}
            
            final_df=add_col_resp["data"]
            load_to_staging_resp = self.loadDataToStaging(final_df)
            if not load_to_staging_resp['success']:
                return {'success': False, 'message' : load_to_staging_resp['message']}
            
            load_to_reporting_resp=self.loadDataToReporting()
            if not load_to_reporting_resp['success']:
                return {'success': False, 'message' : load_to_reporting_resp['message']}
            
            load_to_mip_resp=self.loadDataToMIP()
            if not load_to_mip_resp['success']:
                return {'success': False, 'message' : load_to_mip_resp['message']}
            
            log.info(f'{self.data_file} has been processed.')
            return {"success":True}
        
        except Exception as e:
            log.critical('Error in '+self.processDESWholesalePriceData.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.processDESWholesalePriceData.__name__+f': {e}'}
                 
def pipeline_handler(connection,connection_type):
    log_file = f"deswholesale{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"DesWholesalePrice ETL job started at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
   
    resp = desWholesalePricesETL(engine,conn,cur).processDESWholesalePriceData()
    
    if connection_type in ("Dev",'Production'):  
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
        log.info(f"{connection_type} DesWholesalePrice ETL job ended at {end_timestamp}")
        subject = f'INFO :{connection_type} DesWholesalePrice ETL job status'
        body = f'{connection_type} DesWholesalePrice ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"{connection_type} DesWholesalePrice ETL job ended at {end_timestamp}")
        subject = f'CRITICAL :{connection_type}  DesWholesalePrice ETL job status'
        body = f"{connection_type} DesWholesalePrice ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of deswholesale prices{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
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