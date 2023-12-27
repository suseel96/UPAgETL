# ----------------------------------------------------------------

#                 SEA CNF PRICES DAILY ETL JOB

# -----------------------------------------------------------------

# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import pandas as pd
import numpy as np
import pytz
from etl_utils.utils import emailClient,dbClients,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import time
from selenium.webdriver import Chrome,ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import re

#LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class seaCNFPricesETL():
    def __init__(self,cur,conn,engine):
        self.cur = cur
        self.conn = conn
        self.engine = engine
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgSeaCNFPrices'
        
    def scrapeData(self):
        try:
            log.info("Starting Scraping Data")
            url='https://seaofindia.com/'
            driver_path = '/home/otsietladm/chromedriver'
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  
            chrome_options.add_argument("--no-sandbox")
            browser = Chrome(service=Service(driver_path), options=chrome_options)
            browser.get(url)
            raw_date=browser.find_element(By.XPATH,"//div[@class='quotes-title']").text
            data=browser.find_element(By.XPATH,"//div[@class='quotes-ticker']").get_attribute("outerHTML")
            browser.close()
            df=pd.read_html(data)[0]
            pattern = r'(\d{1,2})(?:st|nd|rd|th)\s?(\w{3})\w*\s(\d{2})'
            match = " ".join(re.search(pattern,raw_date).groups())
            # raw_date = raw_date.replace('th', '').replace("rd","").replace("st","")
            raw_date = datetime.strptime(match, '%d %b %y').strftime('%Y-%m-%d')
            # raw_date = raw_date.strftime('%Y-%m-%d')
            df['SourceDate'] = raw_date
            df.columns=['Price', 'Commodity', 'SourceDate']
            df['Price'] = df['Price'].apply(lambda x: ''.join(x.split()))
            df['CNF Price'] = df['Price'].str.extract('(\d+)')
            df['UOM'] = df.apply(lambda x: x['Price'].replace(str(x['CNF Price']), ''), axis=1)
            df.drop(columns=['Price'], inplace=True)
            df['RequestDate'] = datetime.now().date().strftime('%Y-%m-%d')
            df = df.replace(r'^\s*$', np.nan, regex=True)
            df["Commodity"]=df['Commodity'].str.strip()
            df=df[['Commodity', 'CNF Price', 'UOM', 'SourceDate', 'RequestDate']]
            log.info("Successfully Scrapped Data")
            return {"success":True,"data":df}
        except Exception as e:
            log.critical('Error in '+self.scrapeData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.scrapeData.__name__+f': {e}'}

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
            
    def startSEACNFETL(self):
        try:
            
            scrape_data_resp = self.scrapeData()
            if scrape_data_resp['success']:
                df = scrape_data_resp['data']
            else:
                return {'success' : False, 'message' : scrape_data_resp['message']}
            load_data_to_staging_resp = self.loadDataToStaging(df)
            if not load_data_to_staging_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']}
            
            return {'success': True}
        
        except Exception as e:
            log.critical('Error in '+self.startSEACNFETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startSEACNFETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"seacnf{connection_type}v2.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"SEA CNF Prices ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = seaCNFPricesETL(cur,conn,engine).startSEACNFETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} SEA CNF Prices ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} SEA CNF Prices ETL job status"
        body = f"{connection_type} SEA CNF Prices ETL job succeeded. Please check the attached logs for details."
    else:
        log.critical(f"{connection_type} SEA CNF Prices ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} SEA CNF Prices ETL job status'
        body = f"{connection_type} SEA CNF Prices ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of seacnf{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
    
DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())