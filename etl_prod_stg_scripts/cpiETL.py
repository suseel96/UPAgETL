# ----------------------------------------------------------------

#                 Consumer Price Indices DAILY ETL JOB

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


#LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class cpiPricesETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgcpi'
        self.DB_REPORTING_TABLE_NAME = 'rptcpi'
        self.DB_REPORTING_SCHEMA = 'reporting'
    def generate_dates(self):
        try:
            self.cur.execute('''select max(monthcode) from staging.stgcpi ''')
            max_date = self.cur.fetchall()[0][0]
            start_date = pd.to_datetime(str(max_date), format='%Y%m')
            current_date = pd.to_datetime('now')
            date_sequence = pd.date_range(start=start_date, end=current_date, freq='MS')
            dates =[date.strftime('%Y-%B') for date in date_sequence]
            dates = dates[1:]
            return {'success':True,'dates':dates}
        except Exception as e:
            log.critical('Error in '+self.generate_dates.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.generate_dates.__name__+f': {e}'}
    def scrapeData(self,dates):
        try:
            cpi_df = pd.DataFrame()
            for date in dates:
                year , month = date.split('-')
                log.info("Starting Scraping Data")
                url='https://cpi.mospi.gov.in'
                driver_path = '/home/otsietladm/chromedriver'
                chrome_options = ChromeOptions()
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (often needed in headless mode)
                chrome_options.add_argument("--no-sandbox")
                browser = Chrome(service=Service(driver_path), options=chrome_options)
                browser.get(url)
                url= 'https://cpi.mospi.gov.in/AllIndia_Item_CombinedIndex_2012.aspx'
                browser.get(url)
                select_from_year = browser.find_element(By.XPATH,"//select[@id='Content1_DropDownList1']")
                select_from_year.send_keys(year)
                select_from_month = browser.find_element(By.XPATH,"//select[@id='Content1_DropDownList3']")
                select_from_month.send_keys(month)
                select_to_year = browser.find_element(By.XPATH,"//select[@id='Content1_DropDownList2']")
                select_to_year.send_keys(year)
                select_to_month = browser.find_element(By.XPATH,"//select[@id='Content1_DropDownList4']")
                select_to_month.send_keys(month)
                check_all_button = browser.find_element(By.XPATH,"//a[@id='Content1_lbAll']")
                check_all_button.click()
                view_indicies_button = browser.find_element(By.XPATH,"//input[@type='submit']")
                view_indicies_button.click()
                table_element = browser.find_element(By.XPATH,"//table[@id='Content1_GridView1']").get_attribute('outerHTML')
                df = pd.read_html(table_element)[0]
                # print(df)
                if not df['Month'].unique()[0] == month:
                    log.info(f'No Data Available for {year} and {month}')
                    continue
                if df['Status'].unique() == 'F':
                    df['monthcode'] = pd.to_datetime(df['Month'],format = '%B').dt.month
                    df['monthcode'] = df['monthcode'].apply(lambda x:'0'+str(x) if len(str(x))==1 else x)
                    df['monthcode'] = df['Year'].astype(str) + df['monthcode'].astype(str)
                    df['monthcode'] = df['monthcode'].astype(int)
                    df['requestdate'] = datetime.now().date()
                    cpi_df = pd.concat([cpi_df,df],ignore_index=True)
                    log.info(f'Data Available for {year} and {month}')

                else:
                    log.info(f'No Data Available for {year} and {month}')
            if cpi_df.empty:
                return {'success':False,'message':f'No Latest Data Available.'}
            else:
                return {"success":True,"data":cpi_df}
        except Exception as e:
            log.critical('Error in '+self.scrapeData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.scrapeData.__name__+f': {e}'}

    def loadDataToStaging(self, df):
        try:
        
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
            log.info(f'Data loaded intov{self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToStaging.__name__+f': {e}'}
        
    def loadDataToReporting(self):
        try:
            log.info(f'Loading data into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_TABLE_NAME}')
            self.cur.execute(f'''
            select "QueryText" from reporting."ETLFlowMaster" where "DataSource" = 'CPI' 
            and "UpdateMode" = 'Delta'
            and "SeqNo" =1;
            ''')
            log.info(f'Data loaded into {self.DB_REPORTING_SCHEMA}.{self.DB_REPORTING_TABLE_NAME}')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
            
    def startCpiPricesETL(self):
        try:
            dates_resp = self.generate_dates()
            if dates_resp['success']:
                dates = dates_resp['dates']
            scrape_data_resp = self.scrapeData(dates)
            if scrape_data_resp['success']:
                cpi_col_resp = commonUtills().formatColumnNames(scrape_data_resp['data'])
                if cpi_col_resp['success']:
                    df = cpi_col_resp['data']
            else:
                return {'success' : False, 'message' : scrape_data_resp['message']}
            load_data_to_staging_resp = self.loadDataToStaging(df)
            if not load_data_to_staging_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']}
            load_data_to_reporting_resp = self.loadDataToReporting()
            if not load_data_to_reporting_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']}
            else:
                return {'success': True}
        except Exception as e:
            log.critical('Error in '+self.startCpiPricesETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startCpiPricesETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"consumerpriceindices{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"Consumer Price Indices ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = cpiPricesETL(engine,conn,cur).startCpiPricesETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} Consumer Price Indices ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} Consumer Price Indices ETL job status"
        body = f"{connection_type} Consumer Price Indices ETL job succeeded. Please check the attached logs for details."
    # elif 'No Data Available' in  resp['message']:
    #     conn.close()
    #     os.remove(log_file)
    #     os._exit()
    else:
        log.critical(f"{connection_type} Consumer Price Indices ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} Consumer Price Indices ETL job status'
        body = f"{connection_type} Consumer Price Indices ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of consumerpriceindicies{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    # job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    # emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    # os.remove(log_file)
    conn.close()

DB_CLIENTS = ['STAGING','PRODUCTION']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())