import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import pytz
from dotenv import load_dotenv
from etl_utils.utils import dbClients,emailClient,commonUtills
import logging
from datetime import timedelta, datetime
import requests
import warnings
warnings.filterwarnings('ignore')
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

class internationalPricesETL:
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.name_dict = {'Sunflower oil':'Sunflower Oil', 'RBD Palmolein oil': 'RBD Palmolein (Imported Oil)',
             'Soya oil' : 'Soya Degum (Crude) CIF Mumbai', 'Palm oil':'Palm oil'}
        
    def stagingToReportingUSD(self, commodity_param):
        try:
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'InternationalPrices' and "SeqNo" = 1 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''', {'commodity_param':commodity_param})
            log.info(f'Avaliable USD Prices for {commodity_param} are loaded to reporting.RptInternationalPricesUSD')
            self.conn.commit() 
            return {'success':True}
            
        except Exception as e:
            log.critical('Error in '+self.stagingToReportingUSD.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.stagingToReportingUSD.__name__+f': {e}'}
        
    def reportingToMIPUSD(self,commodity_param):
        try:
            self.cur.execute( """
                    select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'InternationalPrices' and "SeqNo" = 2 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''', {'commodity_param':commodity_param})
            self.conn.commit() 
            log.info(f'Inserted FOB in USD into MIP for {commodity_param}')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'InternationalPrices' and "SeqNo" = 3 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''', {'commodity_param':commodity_param})
            self.conn.commit()
            log.info(f'Inserted CNF in USD into MIP for {commodity_param}')
            return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.reportingToMIPUSD.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.reportingToMIPUSD.__name__+f': {e}'}

    def generateDatesForINR(self,commodity_param):
        try:
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'InternationalPrices' and "SeqNo" = 4 """)
            querytext= self.cur.fetchall()[0][0]
            # self.cur.execute(f'''{querytext}''', {'commodity_param':commodity_param})
            df = pd.read_sql(querytext,params={'commodity_param':commodity_param},con=self.conn)
            if not df.empty:
                dates = sorted(df['generate_series'].tolist())
            else:
                dates = []
            return {'success':True,'dates':dates}
        except Exception as e:
            log.critical('Error in '+self.generateDatesForINR.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.generateDatesForINR.__name__+f': {e}'}

    def stagingToReportingINR(self, commodity_param,dates):
        try:
            for date in dates:
                self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'InternationalPrices' and "SeqNo" = 5 """)
                querytext= self.cur.fetchall()[0][0]
                self.cur.execute(f'''{querytext}''', {'commodity_param':commodity_param,'date':date})
                log.info(f'INR values for {commodity_param} loaded to reporting.RptInternationalPricesINR')
                self.conn.commit() 
            return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.stagingToReportingINR.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.stagingToReportingINR.__name__+f': {e}'}
        
    def reportingToMIPINR(self, commodity_param):
        try:
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'InternationalPrices' and "SeqNo" = 6 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''', {'commodity_param':commodity_param})
            self.conn.commit() 
            log.info(f'Inserted CNF in INR into MIP for {commodity_param}')

            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'InternationalPrices' and "SeqNo" = 7 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''', {'commodity_param':commodity_param})
            self.conn.commit() 
            log.info(f'Inserted Landed Prices in INR into MIP for {commodity_param}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.reportingToMIPINR.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.reportingToMIPINR.__name__+f': {e}'}
            
    def startInternationalPricesETL(self):
        try:
            for commodity_param in self.name_dict.keys():
                stg_repor_usd_resp  = self.stagingToReportingUSD(commodity_param)
                if not stg_repor_usd_resp['success']:
                    return {'success':False,'message':stg_repor_usd_resp['message']}
                repor_mip_usd_resp = self.reportingToMIPUSD(commodity_param)
                if not repor_mip_usd_resp['success']:
                    return {'success':False,'message':repor_mip_usd_resp['message']}
                generate_dates_resp = self.generateDatesForINR(commodity_param)
                if not generate_dates_resp['success']:
                    return {'success':False,'message':generate_dates_resp['message']}
                dates = generate_dates_resp['dates']
                if not dates:
                    log.info(f'No dates to generate landed prices for {commodity_param}')
                    continue
                else:
                    stg_repor_inr_resp = self.stagingToReportingINR(commodity_param,dates)
                    if not stg_repor_inr_resp['success']:
                        return {'success':False,'message':stg_repor_inr_resp['message']}
                    repor_mip_inr_resp = self.reportingToMIPINR(commodity_param)
                    if not repor_mip_inr_resp['success']:
                        return {'success':False,'message':repor_mip_inr_resp['message']}
            return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.startCNFPricesETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startCNFPricesETL.__name__+f': {e}'} 
              
def pipeline_handler(connection,connection_type):
    log_file = f"internationalprices{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"International Prices ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp=internationalPricesETL(engine,conn,cur).startInternationalPricesETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f" {connection_type} International Prices ETL job ended at {end_timestamp}")
        subject = f"INFO :  {connection_type} International Prices ETL job status"
        body = f" {connection_type} International Prices ETL job succeeded. Please check the attached logs for details."
    else:
        log.critical(f" {connection_type} International Prices ETL job ended at {end_timestamp}")
        subject = f"CRITICAL : {connection_type} International Prices ETL job status"
        body = f" {connection_type} International Prices ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of internationalprices{(connection_type)}etl is {round(total_time/60,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)

    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()



DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())