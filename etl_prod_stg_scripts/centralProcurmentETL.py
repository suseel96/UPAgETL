# ----------------------------------------------------------------

#                 procurment DAILY ETL JOB

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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class procurmentETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgfciprocurement'
    def scrapeData(self):
        try:
            log.info("Started Scraping Data")
            url='https://cfpp.nic.in/reportProcurementAndFarmersBenifitted'
            browser_path = '/home/otsietladm/chromedriver'
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (often needed in headless mode)
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--window-size=1920,1080")
            browser = Chrome(service=Service(browser_path), options=chrome_options)
            browser.get(url)
            browser.maximize_window()

            seasons = [season.text for season in Select(browser.find_element(By.XPATH,"//select[@name='marketingSeason']")).options[1:]]
            market_years =[market.text for market in  Select(browser.find_element(By.XPATH,"//select[@name='marketingYear']")).options[1:]]
            commoditys = [commodity.text for commodity in Select(browser.find_element(By.XPATH,"//select[@name='commodityProcuredId']")).options[1:]]
            crop_types = [croptype.text for croptype in Select(browser.find_element(By.XPATH,"//select[@name='cropTypeId']")).options[1:]]
            
            total_df = pd.DataFrame()

            def wait_for_element(locator, timeout=20):
                return WebDriverWait(browser, timeout).until(EC.presence_of_element_located(locator))


            for season in seasons:
                sel_season = Select(wait_for_element((By.XPATH, "//select[@name='marketingSeason']")))
                sel_season.select_by_visible_text(season)
                
                for market_year in market_years:
                    sel_market_year = Select(wait_for_element((By.XPATH, "//select[@name='marketingYear']")))
                    sel_market_year.select_by_visible_text(market_year)
                    
                    for commodity in commoditys:
                        sel_commodity = Select(wait_for_element((By.XPATH, "//select[@name='commodityProcuredId']")))
                        sel_commodity.select_by_visible_text(commodity)
                        
                        for crop_type in crop_types:
                            sel_crop_type = Select(wait_for_element((By.XPATH, "//select[@name='cropTypeId']")))
                            sel_crop_type.select_by_visible_text(crop_type)
                            
                            submit_btn = wait_for_element((By.XPATH, "//input[@value='Submit']"))
                            submit_btn.click()
                            
                            table = wait_for_element((By.XPATH, "/html/body/div/main/div[2]/section/div[3]/div/div/table"))
                            table_html = table.get_attribute('outerHTML')
                            df = pd.read_html(table_html)[0]
                            df['season'] = season
                            df['market_year'] = market_year
                            df['crop'] = commodity
                            df['crop_type'] = crop_type
                            total_df = pd.concat([total_df, df], ignore_index=True)
                    log.info(f'Scrapped data for {season} season market year {market_year}')
            total_df.columns=[col[0] for col in total_df.columns ]
            total_df = total_df.iloc[:,1:]
            total_df['qty_procured_uom'] = 'MT'
            total_df['payment_uom'] = 'LAKHS'
            total_df['request_date'] = datetime.now().date()
            total_df.rename(columns = {'Quantity Procured(MT.)':'Quantity Procured','MSP payment made(in Lacs)':'MSP payment'},inplace=True)
            total_df.loc[total_df['State'].isin(['Grand Total']),'State'] = 'ALL INDIA'
            return {"success":True,"data":total_df}
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
            log.info(f'Loading latest data into reporting.rptfciprourrement')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'FCI' and "SeqNo" = 1 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Loaded latest data into reporting.rptfciprourrement')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
            
    def startprocurmentETL(self):
        try:
            scrape_data_resp = self.scrapeData()
            if not scrape_data_resp['success']:
                    return {'success': False, 'message' : scrape_data_resp['message']}
            df = scrape_data_resp['data']
            col_resp = commonUtills().formatColumnNames(df)
            if not col_resp['success']:
                return {'success': False, 'message' : col_resp['message']}
            load_data_to_staging_resp = self.loadDataToStaging(col_resp['data'])
            if not load_data_to_staging_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']}
            load_data_to_reporting_resp = self.loadDataToReporting()
            if not load_data_to_reporting_resp['success']:
                return {'success': False, 'message' : load_data_to_reporting_resp['message']}
            else:
                return {'success' : True}
        
        except Exception as e:
            log.critical('Error in '+self.startprocurmentETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startprocurmentETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"fciprocurment{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"procurment ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = procurmentETL(engine,conn,cur).startprocurmentETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} fciprocurment ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} fciprocurment ETL job status"
        body = f"{connection_type} fciprocurment ETL job succeeded. Please check the attached logs for details."

    else:
        log.critical(f"{connection_type} fciprocurment ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} fciprocurment ETL job status'
        body = f"{connection_type} fciprocurment ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of fciprocurment{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['STAGING','PRODUCTION']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())