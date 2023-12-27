# ----------------------------------------------------------------

#                 NCDEX DAILY ETL JOB

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


#LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class ncdexPricesETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgncdex'
    def scrapeData(self):
        try:
            log.info("Started Scraping Data")
            # url='https://www.ncdex.com/markets/spotprices'
            driver_path = '/home/otsietladm/chromedriver'
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (often needed in headless mode)
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--window-size=1366,768")
            browser = Chrome(service=Service(driver_path), options=chrome_options)
            browser.maximize_window()
            df = pd.read_sql('''select max(price_date) as max_date from staging.stgncdex''' , self.conn)
            max_date = df['max_date'].iloc[0]
            dates = pd.date_range(start=max_date+timedelta(days=1) ,end= datetime.now().date()-timedelta(days=1),freq='B')
            if dates.empty:
                log.info('Data is upto date')
                return {'success':False,'message':'Data is upto date'}
            else:
                total_df = pd.DataFrame()
                for date in dates:
                    browser.get('https://www.ncdex.com/markets/spotprices')
                    browser.find_element(By.TAG_NAME,'body').send_keys(Keys.CONTROL + Keys.HOME)
                    # browser.save_screenshot('ncdex.png')
                    time.sleep(5)
                    browser.find_element(By.XPATH,"//b[@role='presentation']").click()
                    # browser.save_screenshot('ncdex1.png')
                    browser.find_element(By.XPATH,"//li[contains(text(),'All')]").click()
                    browser.find_element(By.XPATH,"//input[@placeholder='Select from Date']").click()

                    database_month, database_year = date.strftime('%B'), int(date.strftime('%Y'))
                    database_day = str(int(date.strftime('%d')))
                    from_month, from_year = browser.find_element(By.XPATH, "//th[@class='datepicker-switch']").text.split()
                    from_month = datetime.strptime(from_month, '%B').strftime('%B')
                    from_year = int(from_year)
                    
                    if database_year > from_year or (database_year == from_year and database_month > from_month):
                        browser.find_element(By.XPATH,"//th[@class='prev']").click()        
                    elif database_year < from_year or (database_year == from_year and database_month < from_month):
                        browser.find_element(By.XPATH,"//th[@class='next']").click()
                    day_elements = browser.find_elements(By.XPATH, "//td[@class='day' and not(contains(@class, 'old'))]")

                    for day_element in day_elements:
                            if day_element.text == database_day:
                                day_element.click()
                                break
                    
                    browser.find_element(By.XPATH,"//input[@placeholder='Select to Date']").click()
                    to_month,to_year = browser.find_element(By.XPATH,'/html/body/div[3]/div[1]/table/thead/tr[2]/th[2]').text.split()
                    to_month = datetime.strptime(to_month, '%B').strftime('%B')
                    to_year = int(from_year)
                    
                    if database_year > to_year or (database_year == to_year and database_month > to_month):
                        browser.find_element(By.XPATH,"//th[@class='prev']").click()        
                    elif database_year < to_year or (database_year == to_year and database_month < to_month):
                        browser.find_element(By.XPATH,"//th[@class='next']").click()
                        
                    day_elements = browser.find_elements(By.XPATH, "//td[@class='day' and not(contains(@class, 'old'))]")
                    day_elements = browser.find_elements(By.XPATH, "//td[@class='day' and not(contains(@class, 'old')) and not(contains(@class, 'new'))]")
                    for day_element in day_elements:
                        if day_element.text == database_day:
                            day_element.click()
                            break
                    browser.find_element(By.XPATH,"//button[@id='submit']").click()



                    page_height = browser.execute_script("return document.body.scrollHeight")
                    scroll_position = page_height * 1 / 2
                    browser.execute_script(f"window.scrollTo(0, {scroll_position});")

                    time.sleep(8)
                    page = browser.find_element(By.XPATH,"//ul[@class='pagination']")
                    if not page.text:
                        table= browser.find_element(By.XPATH,"//div[@class='dataTables_scrollBody']").get_attribute('outerHTML')
                        df = pd.read_html(table)[0]
                        df = df[~df['Center'].str.contains('No data')]
                        if df.empty:
                            log.info(f'No data avaliable in table for {date}')
                        total_df = pd.concat([total_df,df],ignore_index=True)
                    else:
                        pages = max([int(pa) for pa in page.text.split('\n') if pa.isnumeric()])
                        for page in range(pages):
                            table= browser.find_element(By.XPATH,"//div[@class='dataTables_scrollBody']").get_attribute('outerHTML')
                            df = pd.read_html(table)[0]
                            df = df[~df['Center'].str.contains('No data')]
                            if df.empty:
                                log.info(f'No data avaliable in table for {date}')
                            total_df = pd.concat([total_df, df], ignore_index=True)
                            next_ele =browser.find_element(By.XPATH,"//a[contains(text(),'Next')]")
                            time.sleep(5)
                            if page == pages-1:
                                break
                            else:
                                next_ele.click()
                                time.sleep(5)
                    log.info(f'Data scraped for {date.date()}')
                browser.quit()
                total_df['Price Date'] = total_df['Price Date'].apply(lambda x:datetime.strptime(x,'%d-%b-%Y') if isinstance(x,str) else x.strftime('%Y-%m-%d'))
                total_df['Price Date'] = pd.to_datetime(total_df['Price Date'])
                total_df['price_timestamp'] = total_df['Price Date'].astype(str) +' '+ total_df['Price Time'].astype(str)
                total_df['price_timestamp'] = pd.to_datetime(total_df['price_timestamp'])
                total_df['uom'] = 'Rs/Qtl'
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
            log.info(f'Loading latest data into reporting.rptncdex')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'NCDEX' and "SeqNo" = 1 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Latest data loaded into reporting.rptncdex')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
            
    def startNcdexPricesETL(self):
        try:
            scrape_data_resp = self.scrapeData()
            if scrape_data_resp['success']:
                column_resp = commonUtills().formatColumnNames(scrape_data_resp['data'])
                if column_resp['success']:
                    df = column_resp['data']
                    load_data_to_staging_resp = self.loadDataToStaging(df)
                    if not load_data_to_staging_resp['success']:
                        return {'success': False, 'message' : load_data_to_staging_resp['message']}
                    load_data_to_reporting_resp = self.loadDataToReporting()
                    if not load_data_to_reporting_resp['success']:
                        return {'success': False, 'message' : load_data_to_reporting_resp['message']}
                    else:
                        return {'success': True}
            else:
                return {'success' : False, 'message' : scrape_data_resp['message']}
        
        
        except Exception as e:
            log.critical('Error in '+self.startNcdexPricesETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startNcdexPricesETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"ncdex{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"NCDEX ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']:
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = ncdexPricesETL(engine,conn,cur).startNcdexPricesETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} NCDEX ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} NCDEX ETL job status"
        body = f"{connection_type} NCDEX ETL job succeeded. Please check the attached logs for details."
    elif 'No Data Available' in  resp['message']:
        conn.close()
        os.remove(log_file)
        os._exit()
    else:
        log.critical(f"{connection_type} NCDEX ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} NCDEX ETL job status'
        body = f"{connection_type} NCDEX ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of ncdex{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())