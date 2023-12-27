# ----------------------------------------------------------------

#                 NAFED DAILY ETL JOB

# -----------------------------------------------------------------

# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import pytz
from etl_utils.utils import dbClients,emailClient,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import requests
import time
import re
import warnings
warnings.filterwarnings('ignore')

# LOGGING CONFIGURATION
log = logging.getLogger()

# ETL Code
class nafedETL():
    def __init__(self,engine,conn,cur):
        # self.date = date
        self.season_mapping = {'R':'Rabi','K':'Kharif','S':'Summer'}
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.base_url='https://imsinvpositionapi.neml.in'
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgnafedprocurement'
        self.DB_STAGING_TABLE_NAME1 = 'stgnafedstock'
        self.DB_REPORTING_SCHEMA = 'reporting'
        self.DB_REPORTING_TABLE_NAME = 'rptnafedprocurement'
        self.DB_REPORTING_TABLE_NAME1 = 'rptnafedstock'
#         self.DB_REPORTING_MIP_TABLE_NAME = 'RptMIP'


    def login(self):
        url = f'{self.base_url}/login/authenticate'
        payload = json.dumps({
          "username": "NAFEDAPI",
          "password": "Nafed2023"
        })
        headers = {
          'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        token=response.json()['token']
        return {'success':True,'Token':token}
    
    def stockPosData(self,token):
        try:
            url = f'{self.base_url}/api/stockPosition/getStockPositionData'

            payload = json.dumps({
            
            })
            headers = {
              'Authorization': f'Bearer {token}',
              'Content-Type': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            if response.json()['stockPositionDetails']:
                df=pd.DataFrame.from_records(response.json()['stockPositionDetails'])
                df['year'] = df['seasonId'].apply(lambda x:re.findall(r'\d+',x)[0])
                df['year'] = df['year'].apply(lambda x:datetime.strptime(x,'%y').year if len(x)==2 else x)
                df['year'] = df['year'].astype('int64')
                df['year'] = df['year'].apply(lambda x: f'{x}-{str(x+1)[-2:]}')
                df['lastUpdatedDate'] = pd.to_datetime(df['lastUpdatedDate'])
                df['season'] = df['seasonId'].apply(lambda x:re.findall('[a-zA-Z]',x))
                df['season'] = df['season'].apply(lambda x:x[0] if x else np.nan)
                df['season'] = df['season'].map(self.season_mapping)
                df.loc[~df['season'].isin(['Kharif', 'Rabi', 'Summer']), 'season'] = np.nan
                df.loc[~df['scheme'].isin(['PSS', 'PSF']), 'scheme'] = np.nan
                df['uom_of_qty'] = 'MT'
                df['uom_of_age_of_stock'] = 'Months'
                df['RequestDate'] = datetime.now().date()
                return {'success':True,'data':df}
            else:
                log.info('No Data Avaliable')
                return {'success':False,'message':'No Data Avaliable'}
        except Exception as e:
            log.critical('Error in '+self.stockPosData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.stockPosData.__name__+f': {e}'}
    def getProcData(self,token):
        try:
            url = f'{self.base_url}/api/procurement/getProcurementInventoryData'
            payload = json.dumps({})
            headers = {
              'Authorization': f'Bearer {token}',
              'Content-Type': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            if response.json()['procurementInventoryDetails']:
                df=pd.DataFrame.from_records(response.json()['procurementInventoryDetails'])
                df['procStartDate'] = pd.to_datetime(df['procStartDate'], format='%d/%m/%Y')
                df['procStartDate'] = df['procStartDate'].dt.date
                df['procEndDate'] = pd.to_datetime(df['procEndDate'], format='%d/%m/%Y')
                df['procEndDate'] = df['procEndDate'].dt.date
                df['year'] = df['seasonId'].apply(lambda x:re.findall(r'\d+',x)[0])
                df['year'] = df['year'].apply(lambda x:datetime.strptime(x,'%y').year if len(x)==2 else x)
                df['year'] = df['year'].astype('int64')
                df['year'] = df['year'].apply(lambda x: f'{x}-{str(x+1)[-2:]}')
                df['season'] = df['seasonId'].apply(lambda x:re.findall('[a-zA-Z]',x))
                df['season'] = df['season'].apply(lambda x:x[0] if x else np.nan)
                df['msp'] = df['msp'].astype(float)
                df['sanctionQty'] = df['sanctionQty'].astype(float)
                df['quantityProcuredYesterday'] = df['quantityProcuredYesterday'].astype(float)
                df['lastUpdatedDate'] = pd.to_datetime(df['lastUpdatedDate'])
                df['season'] = df['season'].map(self.season_mapping)
                df.loc[~df['season'].isin(['Kharif', 'Rabi', 'Summer']), 'season'] = None
                df.loc[~df['scheme'].isin(['PSS', 'PSF']), 'scheme'] = None
                df['uom_of_qty'] = 'MT'
                df['price'] = 'Rs/Qtl'
                df['uom_of_no_of_farmers_benifited'] = 'crores'
                df['RequestDate'] = datetime.now().date()
                return {'success':True,'data':df}
            else:
                log.info('No Data Avaliabe')
        except Exception as e:
            log.critical('Error in '+self.getProcData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.getProcData.__name__+f': {e}'}

        
    def loadStockDataToStaging(self,stock_positional_df):
        try:
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME1}')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'NafedStock' and "SeqNo" = 1 """)
            querytext= self.cur.fetchall()[0][0]
            for _, row in stock_positional_df.iterrows():
                self.cur.execute(f'''{querytext}''', row.to_dict())
            self.conn.commit()
            log.info(f'Stock data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME1}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadStockDataToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadStockDataToStaging.__name__+f': {e}'}
        
    def loadProcDataToStaging(self,proc_df):
        try:
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'NafedProcurment' and "SeqNo" = 1 """)
            querytext= self.cur.fetchall()[0][0]
            for _, row in proc_df.iterrows():
                self.cur.execute(f'''{querytext}''', row.to_dict())
            self.conn.commit()
            log.info(f'Procurment data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadProcDataToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadProcDataToStaging.__name__+f': {e}'}
       
    def loadProcDataToReporting(self):
        try:
            self.cur.execute( """
                    select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'NafedProcurment' and "SeqNo" = 2 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext} ''')
            self.conn.commit()
            log.info(f'Procurment data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_TABLE_NAME}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadProcDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadProcDataToReporting.__name__+f': {e}'}
        
    def loadStockDataToReporting(self):
        try:
            self.cur.execute( """
                    select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'NafedStock' and "SeqNo" = 2 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext} ''')
            self.conn.commit()
            log.info(f'Procurment data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_TABLE_NAME1}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadStockDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadStockDataToReporting.__name__+f': {e}'}
       
    def startNAFEDETL(self):
        try:
            token = self.login()
            if token['success']:
                token = token['Token']
            stock_positional_data = self.stockPosData(token)
            if not stock_positional_data['success']:
                return {'success' : False, 'message' : stock_positional_data['message']}
            stock_col_resp = commonUtills().formatColumnNames(stock_positional_data['data'])
            if stock_col_resp['success']:
                stock_positional_df = stock_col_resp['data']
                load_stock_resp = self.loadStockDataToStaging(stock_positional_df)
                if not load_stock_resp['success']:
                    return {'success' : False, 'message' : stock_positional_data['message']}
                
            procurment_data = self.getProcData(token)
            if not procurment_data['success']:
                return {'success' : False, 'message' : procurment_data['message']}
            proc_col_resp = commonUtills().formatColumnNames(procurment_data['data'])
            if proc_col_resp['success']:
                proc_df = proc_col_resp['data']
            load_proc_resp = self.loadProcDataToStaging(proc_df)
            if not load_proc_resp['success']:
                return {'success' : False, 'message' : stock_positional_data['message']}
            load_proc_data_to_rep_resp = self.loadProcDataToReporting()
            if not load_proc_data_to_rep_resp['success']:
                return {'success' : False, 'message' : load_proc_data_to_rep_resp['message']}
            load_stock_data_resp = self.loadStockDataToReporting()
            if not load_stock_data_resp['success']:
                return {'success' : False, 'message' : load_stock_data_resp['message']}
            else:
                return {'success':True}
        except Exception as e:
            log.info(f"NAFED ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
            return {'success': False, 'message' : 'Error in '+self.startNAFEDETL.__name__+f': {e}'}


def pipeline_handler(connection,connection_type):
    log_file = f"nafed{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"NAFED ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    
    resp = nafedETL(engine,conn,cur).startNAFEDETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"NAFED ETL job ended at {end_timestamp}")
        subject = f'INFO :NAFED ETL job status'
        body = f'NAFED ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"NAFED ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : NAFED ETL job status'
        body = f"NAFED ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of  nafed{connection_type}ETL is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()


DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())