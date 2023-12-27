# ----------------------------------------------------------------

#                 DOCA RETAIL PRICES DAILY ETL JOB

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

class docaRetailPricesETL:
    def __init__(self,date,engine,conn,cur):
        self.date = date
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgDocaRetailPrices'
        self.DB_REPORTING_SCHEMA = 'reporting'
        self.DB_REPORTING_TABLE_NAME = 'RptDocaRetailPrices'
        self.DB_REPORTING_MIP_TABLE_NAME = 'RptMIP'
 
    
        
    def extractData(self):
        try:
            # date = (datetime.now().date()-timedelta(days=2)).strftime('%m/%d/%Y')
            log.info(f"Extracting data for {self.date}")
            url = "https://fcainfoweb.nic.in/PMSAPI/api/ReportsRW/GetCenterwisePrices"
            payload = json.dumps({
                                  "AuthKey": "DES23PMSAPI290687",
                                  "Transdate": self.date,
                                  "ReportType": "0"
                                })
								
            headers = {'Content-Type': 'application/json',
                    'Cookie': 'BNI_persistence=JFvcpCchKGQq2wINsuxjzowTzO8F_blsu-gotMhZ4eI10YKc0zKAUG3oon0IPvBJb_dLrjOJ74NuNBcgZjb6gQ=='}
            response = requests.request("POST", url, headers=headers, data=payload)
            if response.json()['Status']!='Success':
                log.critical(f"Data not available for {self.date}")
                return {'success' : False, 'message' : f"Data not available for {self.date}"}
            else:
                df = pd.DataFrame.from_records(response.json()['Content'])
                df['UOM']='Rupees/Kg'
                df['Date'] = pd.to_datetime(df['Transdate1'])
                df.drop(columns=['CenterName_H','CommName_H','Transdate1'],inplace=True)
                log.info(f"Data extracted for {self.date}")
                return {'success' : True, 'data' : df}
        except Exception as e:
            log.critical('Error in '+self.extractData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.extractData.__name__+f': {e}'}
    
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
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'DocaRetail' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =2;
            ''')
            query_text = self.cur.fetchall()[0][0]
            query_date = pd.to_datetime(self.date, format='%m/%d/%Y').strftime('%Y-%m-%d')
            self.cur.execute(query_text, {'date_param':query_date})
            # self.cur.execute(query_text, {'date_param':self.date})
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
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'DocaRetail' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =3;
            ''')
            query_text = self.cur.fetchall()[0][0]
            query_date = pd.to_datetime(self.date, format='%m/%d/%Y').strftime('%Y-%m-%d')
            self.cur.execute(query_text, {'date_param':query_date})
            log.info(f'Data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_MIP_TABLE_NAME}')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToMIP.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToMIP.__name__+f': {e}'}
    
    def startDocaRetailETL(self):
        try:
            
            extract_data_resp = self.extractData()
            if extract_data_resp['success']:
                df = extract_data_resp['data']
            else:
                return {'success' : False, 'message' : extract_data_resp['message']}
           
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
            log.critical('Error in '+self.startDocaRetailETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startDocaRetailETL.__name__+f': {e}'}

def maxDate(cur):
    cur.execute('''
                   select max("Date") from staging."stgDocaRetailPrices"
    ''')
    max_date=cur.fetchall()[0][0]
    date_range = pd.date_range(start=(max_date+timedelta(days=1)), end=(datetime.now().date()))
    date_range=[date.strftime(format='%m/%d/%Y').upper() for date in date_range]
    return {'success':True,'daterange':date_range}

def delFiveDays(cur,conn):
        try:
            cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'DocaRetail' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =1;
            ''')
            query_text = cur.fetchall()[0][0]
            cur.execute(query_text)
            conn.commit()
            log.info('Deleted last five days data')
            return {'success':True}
        except Exception as e:
            log.critical('Error in '+delFiveDays.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+delFiveDays.__name__+f': {e}'}

def pipeline_handler(connection,connection_type):
    log_file = f"docaretail{(connection_type).lower()}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"Doca Retail Prices ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    del_data_resp = delFiveDays(cur,conn)
    if not del_data_resp['success']:
        return {'success' : False, 'message' : del_data_resp['message']}
    dates_resp = maxDate(cur)
    if dates_resp['success']:
        dates=dates_resp['daterange']
    if dates:
        for date in dates:
            resp = docaRetailPricesETL(date,engine,conn,cur).startDocaRetailETL()
            end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
            if resp['success']:
                log.info(f"{connection_type} Doca Retail Prices ETL job ended at {end_timestamp}")
                subject = f"INFO :{connection_type} Doca Retail Prices ETL job status"
                body = f"{connection_type} Doca Retail Prices ETL job succeeded. Please check the attached logs for details."
            else:
                log.critical(f"{connection_type} Doca Retail Prices ETL job ended at {end_timestamp}")
                subject = f"CRITICAL :{connection_type} Doca Retail Prices ETL job status"
                body = f"{connection_type} Doca Retail Prices ETL job failed. Error : {resp['message']} Please check the attached logs for details."
        total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
        log.info(f"Execution time of docaretail{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
        job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
        emailClient().send_mail(subject, body, log_file)        
        log.handlers.clear()
        os.remove(log_file)
        conn.close()
    else:
        log.handlers.clear()
        os.remove(log_file)
        conn.close()

DB_CLIENTS =['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())
