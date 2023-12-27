import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import pytz
from etl_utils.db_client import connectToProductionDb,connectToStagingDb
from dotenv import load_dotenv
from etl_utils.email_client import send_mail,email_sender_receiver
import logging
from datetime import timedelta, datetime
import requests
import warnings
warnings.filterwarnings('ignore')
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

# EMAIL CONFIGURATION
EMAIL_SENDER,EMAIL_RECEIVER,EMAIL_PASSWORD = email_sender_receiver()

class cnfPriceETL:
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.name_dict = {'Sunflower oil':'Sunflower Oil', 'RBD oil': 'RBD Palmolein (Imported Oil)',
             'Soya oil' : 'Soya Degum (Crude) CIF Mumbai', 'Palm oil':'Palm oil'}
        self.uom_dict = {'Sunflower oil':'(INR/MT)', 'RBD oil': '(INR/MT)',
             'Soya oil' : '(USD/MT)', 'Palm oil':'(INR/MT)'}
        self.uom_types = ['(USD/MT)', '(INR/MT)']
    def formulateCNFinActuals(self, commodity_param):
        try:
            if commodity_param != 'Palm oil':
                query = f'''select distinct "Commodity","CNF Price","SourceDate" from staging."stgSeaCNFPrices"
                    where "Commodity" = '{self.name_dict[commodity_param]}'
                    and "SourceDate" >= (
                    SELECT max("Date") FROM staging."formulatedSeaCNFPricesNew"
                    where "Commodity" = '{commodity_param}'
                    and "UOM" = '{self.uom_dict[commodity_param]}') '''
            else:
                query = f'''select 'Palm oil' "Commodity", "AveragePrice_I_II" "CNF Price",
                        "Date_YYYYMMDD"::date "SourceDate"
                        from staging."stgCPONMEO"
                        where "Date_YYYYMMDD"::date >= (
                        SELECT max("Date") FROM staging."formulatedSeaCNFPricesNew"
                        where "Commodity" = 'Palm oil'
                        and "UOM" = '{self.uom_dict[commodity_param]}') '''
            df = pd.read_sql(query, self.conn)
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
                # data_df.to_excel(f'''{commodity_param}_{self.uom_dict[commodity_param][1:4]}.xlsx''', index=False)
                data_df.to_sql('formulatedSeaCNFPricesNew',self.engine, schema = 'staging', if_exists='append', index=False)
                self.conn.commit()
                log.info(f'CNF Prices in {self.uom_dict[commodity_param]} generated for {commodity_param}')
            return {'success' :True}
        except Exception as e:
            log.critical('Error in '+self.formulateCNFinActuals.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.formulateCNFinActuals.__name__+f': {e}'}
            
    def formulateCNFinAlternateUOM(self, commodity_param):
        try:
            required_uom = [i for i in self.uom_types if self.uom_dict[commodity_param]!=i][0]
            query = f'''with exis_data as (select * FROM staging."formulatedSeaCNFPricesNew"
                        where "Commodity" = '{commodity_param}' and "UOM" = '{self.uom_dict[commodity_param]}' 
                        and "Date" > (select max("Date") FROM staging."formulatedSeaCNFPricesNew"
                        where "Commodity" = '{commodity_param}' and "UOM" = '{required_uom}' ) )
                        select exis_data.*, ffer.rate from exis_data
                        left join staging."formulatedFbilExchRates" ffer
                        on exis_data."Date" = ffer."Date"::date
                        where ffer."Date" is not Null
                        order by exis_data."Date" '''
            alternate_uom_dates = pd.read_sql(query,self.conn)
            if not alternate_uom_dates.empty:
                # alternate_uom_dates = alternate_uom_dates[~alternate_uom_dates['rate'].isna()]
                if alternate_uom_dates['UOM'].unique()[0] == '(INR/MT)':
                    alternate_uom_dates['CNF Price'] = alternate_uom_dates['CNF Price']/alternate_uom_dates['rate']
                else:
                    alternate_uom_dates['CNF Price'] = alternate_uom_dates['CNF Price']*alternate_uom_dates['rate']
                alternate_uom_dates['UOM'] = required_uom
                alternate_uom_dates = alternate_uom_dates[['Commodity', 'Date', 'UOM', 'CNF Price']]
                alternate_uom_dates.to_sql('formulatedSeaCNFPricesNew',self.engine, schema = 'staging', if_exists='append', index=False)
                log.info(f'Alternate CNF Price in {required_uom} generated for {commodity_param}')
                return {'success':True}
            else:
                log.info(f'No Exchange rates to generate Alternate UOM {required_uom} for {commodity_param}')
        except Exception as e:
            log.critical('Error in '+self.formulateCNFinAlternateUOM.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.formulateCNFinAlternateUOM.__name__+f': {e}'}
            
    def startCNFPricesETL(self):
        try:
            log.info(f"CNF Prices ETL job triggered at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
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
              
def pipeline_handler(connection):
    log_file = f"cnfprices{(str(connection).split(' ')[1][9:-2]).lower()}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp=cnfPriceETL(engine,conn,cur).startCNFPricesETL()
    if resp['success']:
        log.info(f" {str(connection).split(' ')[1][9:-2]} CNF Prices ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f'INFO :  {str(connection).split(" ")[1][9:-2]} CNF Prices ETL job status'
        body = f' {str(connection).split(" ")[1][9:-2]} CNF Prices ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f" {str(connection).split(' ')[1][9:-2]} CNF Prices ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f'CRITICAL : {str(connection).split(" ")[1][9:-2]} CNF Prices ETL job status'
        body = f" {str(connection).split(' ')[1][9:-2]} CNF Prices ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = time.time() - start_time   
    # log.info(f"Execution time of CNF Prices{(str(connection).split(' ')[1][9:-2]).lower()}etl is {str(total_time)} Seconds")
    log.info(f"Execution time of CNF Prices {(str(connection).split(' ')[1][9:-2]).lower()}etl is {str(total_time/60)} Minutes")
    send_mail(subject, body, log_file, EMAIL_SENDER, EMAIL_RECEIVER, EMAIL_PASSWORD)
    log.handlers.clear()
    # os.remove(log_file)
    conn.close()


start_time = time.time()
pipeline_handler(connectToStagingDb)
# start_time = time.time()
# pipeline_handler(connectToProductionDb)

            