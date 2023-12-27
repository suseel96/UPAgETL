# ----------------------------------------------------------------

#                 usda DAILY ETL JOB

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
class usdaPricesETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgusda'

    def deleteData(self):
        try:
            log.info(f'Deleting max 2 years data from usda tables')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'USDA' and "SeqNo" = 1""")
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Deleted max 2 years data from the usda tables')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.deleteData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.deleteData.__name__+f': {e}'}
        
    def scrapeData(self):
        try:
            log.info("Started Scraping Data")
            url='https://apps.fas.usda.gov/psdonline/app/index.html#/app/downloads'
            driver_path = '/home/otsietladm/chromedriver'
            download_path = '/home/otsietladm/etl_utils/temp_downloads/usda'
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
            browser.get(url)
            
            psd_link = browser.find_element(By.XPATH, '/html/body/div[2]/div[3]/div/div[2]/div/ul/li[3]/a/uib-tab-heading/i')
            time.sleep(5)
            psd_link.click()

            csv_link = browser.find_element(By.XPATH, "//a[@class='ng-binding' and contains(text(),'psd_alldata_csv.zip')]")
            time.sleep(10)
            csv_link.click()

            # Wait for the file to be downloaded
            zip_file_path = os.path.join(download_path, 'psd_alldata_csv.zip')
            timeout = 300  # Set a reasonable timeout (in seconds)
            interval = 10   # Check for the file every 10 seconds

            while not os.path.exists(zip_file_path) and timeout > 0:
                time.sleep(interval)
                timeout -= interval

            if os.path.exists(zip_file_path):
                log.info('Downloaded the zip file')
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(download_path)
                    log.info('Extracted the data from the zipfile')

                file_path = os.path.join(download_path, 'psd_alldata.csv')
                log.info('Reading the data from the file')
                df = pd.read_csv(file_path)
                max_avail_year = pd.read_sql('''select max("Market_Year" ) from staging.stgusda suv ''' , self.conn)['max'].values[0]
                print(max_avail_year)
                df['Market_Year'] = df['Market_Year'].astype(int)
                df = df[(df['Market_Year'] > int(max_avail_year)) & (df['Market_Year'] <= datetime.now().year)]
                log.info('loading data into staging.stgusda')
                df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
                log.info('loaded data into staging.stgusda')

                os.remove(zip_file_path)
                log.info('Deleted the Zip file')
                os.remove(file_path)
                log.info('Deleted the data file')
            else:
                log.info("Timeout: File download took too long.")

            browser.quit()

            return {"success":True,"data":df}
        except Exception as e:
            log.critical('Error in '+self.scrapeData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.scrapeData.__name__+f': {e}'}
        
    def loadDataToReporting(self):
        try:
            log.info(f'Loading latest data into reporting.rptusda')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'USDA' and "SeqNo" = 2 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Latest data loaded into reporting.rptusda')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
        
    def loadDataToMIP(self):
        try:
            log.info(f'Loading latest data into reporting.RptMIP')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'USDA' and "SeqNo" = 3 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Latest data loaded into reporting.RptMIP')
            self.conn.commit()

            log.info(f'Loading latest world area and production data into reporting.RptMIP')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'USDA' and "SeqNo" = 4 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Loaded latest area and production data into reporting.RptMIP')
            self.conn.commit()

            log.info(f'Loading latest world yield data into reporting.RptMIP')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'USDA' and "SeqNo" = 5 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Loaded latest yield data into reporting.RptMIP')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToMIP.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToMIP.__name__+f': {e}'}
            
    def startUsdaPricesETL(self):
        try:
            del_resp = self.deleteData()
            if not del_resp['success']:
                return {'success' : False, 'message' : del_resp['message']}
            scrape_data_resp = self.scrapeData()
            if not scrape_data_resp['success']:
                return {'success' : False, 'message' :scrape_data_resp['message']}
            load_data_repor_resp = self.loadDataToReporting()
            if not load_data_repor_resp['success']:
                return {'success' : False, 'message' :load_data_repor_resp['message']}
            load_data_to_mip_resp = self.loadDataToMIP()
            if not load_data_to_mip_resp['success']:
                return {'success' : False, 'message' :load_data_to_mip_resp['message']}
            else:
                return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.startUsdaPricesETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startUsdaPricesETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"usda{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"usda ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = usdaPricesETL(engine,conn,cur).startUsdaPricesETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} usda ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} usda ETL job status"
        body = f"{connection_type} usda ETL job succeeded. Please check the attached logs for details."
    elif 'No Data Available' in  resp['message']:
        os.remove(log_file)
        os._exit()
    else:
        log.critical(f"{connection_type} usda ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} usda ETL job status'
        body = f"{connection_type} usda ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of usda{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
    
DB_CLIENTS = ['STAGING','PRODUCTION']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())