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
import warnings
warnings.filterwarnings('ignore')
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

class cnfPriceETL:
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.name_dict = {'Sunflower oil':'Sunflower Oil', 'RBD Palmolein oil': 'RBD Palmolein (Imported Oil)',
             'Soya oil' : 'Soya Degum (Crude) CIF Mumbai', 'Palm oil':'Palm oil'}
        self.uom_dict = {'Sunflower oil':'(INR/MT)', 'RBD Palmolein oil': '(INR/MT)',
             'Soya oil' : '(USD/MT)', 'Palm oil':'(INR/MT)'}
        self.uom_types = ['(USD/MT)', '(INR/MT)']
    def formulateCNFinActuals(self, commodity_param):
        try:
            if commodity_param != 'Palm oil':
                self.cur.execute( '''select "QueryText"  from reporting."ETLFlowMaster" em where em."DataSource" ='CNFPrices' and "SeqNo" =1 ''')
                querytext= self.cur.fetchall()[0][0].format(raw_name=self.name_dict[commodity_param],std_name=commodity_param,UOM=self.uom_dict[commodity_param])
            else:
                self.cur.execute( '''select "QueryText"  from reporting."ETLFlowMaster" em where em."DataSource" ='CNFPrices' and "SeqNo" =2 ''')
                querytext= self.cur.fetchall()[0][0].format(UOM=self.uom_dict[commodity_param])
            df = pd.read_sql(querytext, self.conn)
            if df['SourceDate'].nunique() == 1:
                log.info(f'No dates to formulate CNF Price for {commodity_param} in {self.uom_dict[commodity_param]}')
            else:
                df['UOM'] = self.uom_dict[commodity_param]
                min_date = str(df['SourceDate'].min())
                max_date = str(df['SourceDate'].max())
                df['SourceDate'] = df['SourceDate'].apply(str)
                dates_df = pd.DataFrame(pd.date_range(start=min_date, end = max_date), columns = ['SourceDate'])
                dates_df['SourceDate'] = dates_df['SourceDate'].apply(lambda x: str(x)[:10])
                data_df = dates_df.merge(df, on = 'SourceDate', how='left')
                data_df['SourceDate'] = pd.to_datetime(dates_df['SourceDate'])
                data_df.sort_values('SourceDate', inplace=True)
                data_df.ffill(inplace=True)
                data_df = data_df[data_df['SourceDate'] != min_date]
                data_df.rename(columns = {'SourceDate':'Date'}, inplace=True)
                data_df['Commodity'] = commodity_param
                data_df.to_sql('formulatedSeaCNFPrices',self.engine, schema = 'staging', if_exists='append', index=False)
                self.conn.commit()
                log.info(f'CNF Prices in {self.uom_dict[commodity_param]} generated for {commodity_param}')
            return {'success' :True}
        except Exception as e:
            log.critical('Error in '+self.formulateCNFinActuals.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.formulateCNFinActuals.__name__+f': {e}'}
            
    def formulateCNFinAlternateUOM(self, commodity_param):
        try:
            required_uom = [i for i in self.uom_types if self.uom_dict[commodity_param]!=i][0]
            self.cur.execute( '''select "QueryText"  from reporting."ETLFlowMaster" em where em."DataSource" ='CNFPrices' and "SeqNo" =3 ''')
            querytext= self.cur.fetchall()[0][0].format(commodity_param=commodity_param,UOM=self.uom_dict[commodity_param],required_uom=required_uom)
            alternate_uom_dates = pd.read_sql(querytext,self.conn)
            if not alternate_uom_dates.empty:
                if alternate_uom_dates['UOM'].unique()[0] == '(INR/MT)':
                    alternate_uom_dates['CNF Price'] = alternate_uom_dates['CNF Price']/alternate_uom_dates['rate']
                else:
                    alternate_uom_dates['CNF Price'] = alternate_uom_dates['CNF Price']*alternate_uom_dates['rate']
                alternate_uom_dates['UOM'] = required_uom
                alternate_uom_dates = alternate_uom_dates[['Commodity', 'Date', 'UOM', 'CNF Price']]
                alternate_uom_dates.to_sql('formulatedSeaCNFPrices',self.engine, schema = 'staging', if_exists='append', index=False)
                self.conn.commit()
                log.info(f'Alternate CNF Price in {required_uom} generated for {commodity_param}')
                return {'success':True}
            else:
                log.info(f'No Exchange rates to generate Alternate UOM {required_uom} for {commodity_param}')
        except Exception as e:
            log.critical('Error in '+self.formulateCNFinAlternateUOM.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.formulateCNFinAlternateUOM.__name__+f': {e}'}
            
    def startCNFPricesETL(self):
        try:
            for commodity_param in self.name_dict.keys():
                formulate_cnf_actual_uom_resp = self.formulateCNFinActuals(commodity_param)
                if not formulate_cnf_actual_uom_resp['success']:
                    return {'success':False}
                else:
                    formulate_cnf_alternate_uom_resp = self.formulateCNFinAlternateUOM(commodity_param)
            return {'success':True}
                
        except Exception as e:
            log.critical('Error in '+self.startcnfpricesETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startcnfpricesETL.__name__+f': {e}'} 
              
def pipeline_handler(connection,connection_type):
    # connection_type=str(connection).split(' ')[2][18:-2]
    
    log_file = f"cnfpricesV2{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"CNF Prices ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp=cnfPriceETL(engine,conn,cur).startCNFPricesETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f" {connection_type} CNF Prices ETL job ended at {end_timestamp}")
        subject = f'INFO :  {connection_type} CNF Prices ETL job status'
        body = f' {connection_type} CNF Prices ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f" {connection_type} CNF Prices ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} CNF Prices ETL job status'
        body = f" {connection_type} CNF Prices ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of cnfprices{connection_type}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()


DB_CLIENTS = ['STAGING','PRODUCTION']
for client in DB_CLIENTS:
    start_time = time.time()
    pipeline_handler(dbClients(client).connectToDb,client.title())