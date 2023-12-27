# ----------------------------------------------------------------

#                 ENAM DAILY ETL JOB

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
from etl_utils.utils import emailClient,dbClients,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import requests
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

# ETL Code
class enamETL():
    def __init__(self,date,engine,conn,cur):
        self.date = date
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.base_url='https://enam.gov.in/StateBoardWebSrv/'
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgENAM'
        self.DB_REPORTING_SCHEMA = 'reporting'
        self.DB_REPORTING_TABLE_NAME = 'RptENAM'
        self.DB_REPORTING_MIP_TABLE_NAME = 'RptMIP'

    def getCommodityList(self):
        try:
            log.info(f"Fetching commodity list.")
            url = self.base_url+"rest/stateboard/getCommodity"
            headers = {
                          'Content-Type': 'application/x-www-form-urlencoded',
                          'Cookie': 'JSESSIONID=EukPgZJR3BQXrxPiVwNzO7Bk.undefined; SERVERID=node44'
                      }
            response = requests.request("POST", url, headers=headers)
            commodity_df = pd.DataFrame.from_records(response.json()['commodityDetails'])
            log.info(f"Commodity list extracted.")
            return {'success':True,'data':commodity_df}
        except Exception as e:
            log.critical('Error in '+self.getCommodityList.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.getCommodityList.__name__+f': {e}'}
            
    def getModalPrice(self,commodity_df):
        try:
            log.info(f"Fetching arrivals and prices data.")
            payload = {'getModalPrice':
                json.dumps({'getModalPrice':{'toDate':self.date,'fromDate':self.date,'stateList':commodity_df['state'].unique().tolist(),'apmcList':commodity_df['apmc'].unique().tolist(),'commodityList':commodity_df['commodity'].unique().tolist()}})}
            url = self.base_url+"rest/stateboard/getModalPrice"
            headers = {
                          'Content-Type': 'application/x-www-form-urlencoded',
                          'Cookie': 'JSESSIONID=EukPgZJR3BQXrxPiVwNzO7Bk.undefined; SERVERID=node44'
                      }
            response = requests.request("POST", url, headers=headers, data=payload)
            if response.json()["statusMsg"]!='Sorry, no data found':
                df=pd.DataFrame.from_records(response.json()['tradeDetails'])
            df.rename(columns={'date':'requestDate'},inplace=True)
            df['quantityUOM']='Quintal'
            df['PriceUOM']='Rs/Qtl'
            log.info(f"Data fetched for {self.date}")
            return {'success':True,'data':df}
        except Exception as e:
            log.critical('Error in '+self.getModalPrice.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.getModalPrice.__name__+f': {e}'}
    
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
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'ENAM' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =1;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text, {'date_param':self.date})
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
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'ENAM' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =2;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text, {'date_param':self.date})
            self.conn.commit()
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'ENAM' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =3;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text, {'date_param':self.date})
            self.conn.commit()
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'ENAM' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =4;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text, {'date_param':self.date})
            self.conn.commit()
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'ENAM' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =5;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text, {'date_param':self.date})
            self.conn.commit()
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'ENAM' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =6;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text, {'date_param':self.date})
            self.conn.commit()
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'ENAM' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =7;
            ''')
            query_text = self.cur.fetchall()[0][0]
            self.cur.execute(query_text, {'date_param':self.date})
            self.conn.commit()
            log.info(f'Data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_MIP_TABLE_NAME}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToMIP.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToMIP.__name__+f': {e}'} 


    def startEnamETL(self):
        try:
            extract_data_resp = self.getCommodityList()
            if not extract_data_resp['success']:
                return {'success' : False, 'message' : extract_data_resp['message']}
            commodity_df = extract_data_resp['data']
            modal_price=self.getModalPrice(commodity_df)
            if not modal_price['success']:
                return {'success' : False, 'message' : modal_price['message']}
            df=modal_price['data']
            load_data_to_staging_resp = self.loadDataToStaging(df)
            if not load_data_to_staging_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']}
            load_data_to_rpt_resp = self.loadDataToReporting()
            if not load_data_to_rpt_resp['success']:
                return {'success': False, 'message' : load_data_to_rpt_resp['message']}
            load_data_to_mip_resp = self.loadDataToMIP()
            if not load_data_to_mip_resp['success']:
                return {'success': False, 'message' : load_data_to_mip_resp['message']}
            else:
                return {'success': True}
        except Exception as e:
            log.info(f"ENAM ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
            return {'success': False, 'message' : 'Error in '+self.startEnamETL.__name__+f': {e}'}

def maxDate(cur):
    cur.execute('''
                    select max("tranDate") from staging."stgENAM"
    ''')
    max_date=cur.fetchall()[0][0]
    date_range = pd.date_range(start=(max_date+timedelta(days=1)), end=(datetime.now().date()-timedelta(days=1)))
    date_range=[date.strftime(format='%d-%m-%Y') for date in date_range]
    return {'success':True,'daterange':date_range}

def pipeline_handler(connection,connection_type):
    log_file = f"enam{(connection_type).lower()}etl.log"
    root = logging.getLogger()
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"ENAM ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    
    dates_resp = maxDate(cur)
    if dates_resp['success']:
        dates=dates_resp['daterange']
    if dates:
        for date in dates:
            resp = enamETL(date,engine,conn,cur).startEnamETL()
            end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
            if resp['success']:
                log.info(f"{connection_type} ENAM ETL job ended at {end_timestamp}")
                subject = f"INFO :{connection_type} ENAM ETL job status"
                body = f"{connection_type} ENAM ETL job succeeded. Please check the attached logs for details."
            else:
                log.critical(f"{connection_type} ENAM ETL job ended at {end_timestamp}")
                subject = f"CRITICAL : {connection_type} ENAM ETL job status"
                body = f"{connection_type} ENAM ETL job failed. Error : {resp['message']} Please check the attached logs for details."
        total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
        log.info(f"Execution time of enam{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
        job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
        emailClient().send_mail(subject, body, log_file)
        log.handlers.clear()
        os.remove(log_file)
        conn.close()
    else:
        os.remove(log_file)
        conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())