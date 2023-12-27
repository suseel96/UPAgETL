# ----------------------------------------------------------------

#                 rainfall DAILY ETL JOB

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
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys


#LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class rainfallETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgrainfall'
    def scrapeData(self):
        try:
            log.info("Started Scraping Data")
            url='https://vedas.sac.gov.in/krishi/dashboard/index.html'
            browser_path = '/home/otsietladm/chromedriver'
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (often needed in headless mode)
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--window-size=1920,1080")
            browser = Chrome(service=Service(browser_path), options=chrome_options)
            browser.get(url)
            browser.maximize_window()

            dates = pd.read_sql('''select max("Date") from staging.stgrainfall''', self.conn)
            dates = pd.date_range(start=dates['max'].values[0]+timedelta(days=1),end = datetime.now().date())
            dates = [date.strftime('%Y-%m-%d') for date in dates]

            cumulative_rainfall = pd.DataFrame()
            cumulative_thesil_rainfall = pd.DataFrame()
            dist_teshil = Select(browser.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/select'))
            for option in dist_teshil.options:
                op = option.text
                option.click()
                next_drop_down = Select(browser.find_element(By.XPATH,'/html/body/div/div[2]/div[3]/select'))
                for drop in next_drop_down.options[:1]:
                    dr=drop.text
                    drop.click()
                    for date in dates:
                        browser.find_element(By.XPATH,'/html/body/div/div[2]/div[5]/span[2]/select').send_keys(date)
                        time.sleep(20)
                        browser.find_element(By.XPATH,'/html/body/div/div[4]/div[2]/div[1]/div/div[1]').click()
                        table_drop_down = Select(browser.find_element(By.XPATH,'/html/body/div/div[8]/div/div[2]/select'))
                        for table_option in table_drop_down.options:
                            try:
                                table_option.click()
                                table_data = browser.find_element(By.XPATH,'/html/body/div/div[8]/div/div[4]/table').get_attribute('outerHTML')
                                df = pd.read_html(table_data)[0]
                                df['rainfall_type'] = table_option.text
                                df['Date'] = date
                                if op == 'District' and dr == 'Cumulative Rainfall (mm)':
                                    cumulative_rainfall = pd.concat([cumulative_rainfall,df],ignore_index=True)
                                elif op == 'Tehsil' and dr== 'Cumulative Rainfall (mm)':
                                    cumulative_thesil_rainfall = pd.concat([cumulative_thesil_rainfall,df],ignore_index=True)
                            except:
                                continue
                        log.info(f' scraped data for the {op} on {date} ')
                        browser.find_element(By.XPATH,'/html/body/div/div[8]/div/div[1]/span').click()
            cumulative_rainfall = cumulative_rainfall.drop_duplicates()
            cumulative_thesil_rainfall = cumulative_thesil_rainfall.drop_duplicates()
            cumulative_rainfall['Tehsil'] = np.nan
            cumulative_rainfall['location_type'] = 'district'
            cumulative_thesil_rainfall['location_type'] = 'Thesil'
            final_df = pd.concat([cumulative_rainfall,cumulative_thesil_rainfall],ignore_index=True)
            final_df['uom'] = 'mm'

            return {"success":True,"data":final_df}
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
            log.info(f'Loading latest data into reporting.rptrainfall')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'rainfall' and "SeqNo" = 1 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Latest data loaded into reporting.rptrainfall')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
            
    def startrainfallETL(self):
        try:
            scrape_data_resp = self.scrapeData()
            if not scrape_data_resp['success']:
                    return {'success': False, 'message' : scrape_data_resp['message']}
            df = scrape_data_resp['data']
            load_data_to_staging_resp = self.loadDataToStaging(df)
            if not load_data_to_staging_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']}
            else:
                return {'success' : True}
        
        except Exception as e:
            log.critical('Error in '+self.startrainfallETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startrainfallETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"rainfall{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    log.info(f"rainfall ETL job triggered at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = rainfallETL(engine,conn,cur).startrainfallETL()
    if resp['success']:
        log.info(f"{connection_type} rainfall ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f"INFO : {connection_type} rainfall ETL job status"
        body = f"{connection_type} rainfall ETL job succeeded. Please check the attached logs for details."
    elif 'No Data Available' in  resp['message']:
        conn.close()
        os.remove(log_file)
        os._exit()
    else:
        log.critical(f"{connection_type} rainfall ETL job ended at {datetime.now(pytz.timezone('Asia/Kolkata'))}")
        subject = f'CRITICAL : {connection_type} rainfall ETL job status'
        body = f"{connection_type} rainfall ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = time.time() - start_time
    log.info(f"Execution time of rainfall{(connection_type).lower()}etl is {str(round(total_time/60,3))} Minutes")
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS[:1]:
    start_time = time.time()
    pipeline_handler(dbClients(client).connectToDb,client.title())