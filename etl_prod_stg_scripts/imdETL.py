# ----------------------------------------------------------------

#                 imd DAILY ETL JOB

# -----------------------------------------------------------------

# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import pandas as pd
import numpy as np
import pytz
from etl_utils.utils import dbClients,emailClient,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import time
from selenium.webdriver import Chrome,ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
import zipfile

#LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class imdETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgimdagrimet'

    
    def scrapeData(self):
        try:
            log.info("Started Scraping Data")
            driver_path = '/home/otsietladm/chromedriver'
            download_path = '/home/otsietladm/etl_utils/temp_downloads/imdagrimet'

            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (often needed in headless mode)
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--window-size=1366,768")
            chrome_options.add_experimental_option('prefs', {
                                        'download.default_directory': download_path,
                                        'download.prompt_for_download': False,
                                        'download.directory_upgrade': True,
                                        'safebrowsing.enabled': True
                                    })
            browser = Chrome(service=Service(driver_path), options=chrome_options)
            browser.maximize_window()

            max_date = pd.read_sql('''select max(observation_dt) from staging.stgimdagrimet''' , self.conn)['max'].values[0]
            max_date=pd.to_datetime(max_date)
            dates = pd.date_range(max_date + timedelta(days=1) , datetime.now().date() - timedelta(days=1))
            dates = [date.strftime('%Y-%m-%d') for date in dates]

            for date_param in dates:
                url=f'https://imdagrimet.gov.in/surfacecsv.php?date={date_param}'
                
                browser.get(url)
                time.sleep(3)
                df = pd.read_csv(download_path+'/'+'Data.csv')
                if not df.empty:
                    log.info(f'Downloaded file for {date_param}')
                    df.dropna(how='all', axis=1, inplace=True)
                    df.columns = df.columns.str.strip()
                    df.to_sql('stgimdagrimet', self.engine, schema='staging', if_exists="append", index=False, chunksize=50000)
                    log.info('Loaded data into staging.stgimdagrimet')
                    self.conn.commit()
                    time.sleep(2)
                os.remove(download_path+'/'+'Data.csv')

            # Close the browser
            browser.quit()

            return {"success":True}
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
        
    def loadDataToReporting(self):
        try:
            log.info(f'Loading latest data into reporting.rptimd')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'imd' and "SeqNo" = 1 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Latest data loaded into reporting.rptimd')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
            
    def startImdETL(self):
        try:
            scrape_data_resp = self.scrapeData()
            if not scrape_data_resp['success']:
                return {'success' : False, 'message' : scrape_data_resp['message']}
            else:
                return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.startImdETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startImdETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"imd{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"IMD ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = imdETL(engine,conn,cur).startImdETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} IMD ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} imd ETL job status"
        body = f"{connection_type} imd ETL job succeeded. Please check the attached logs for details."
    elif 'No Data Available' in  resp['message']:
        conn.close()
        os.remove(log_file)
        os._exit()
    else:
        log.critical(f"{connection_type} IMD ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} imd ETL job status'
        body = f"{connection_type} IMD ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of imd{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
    
DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS[1:]:
    pipeline_handler(dbClients(client).connectToDb,client.title())