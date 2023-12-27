# ----------------------------------------------------------------

#          Normalized Difference Vegetation Index ETL JOB

# -----------------------------------------------------------------
#Imports
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
import warnings
warnings.filterwarnings('ignore')
import time

from selenium.webdriver import Chrome,ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select,WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd

# LOGGING CONFIGURATION
log = logging.getLogger()

class ndviETL:
    def __init__(self, engine, conn, cur):
        self.engine = engine
        self.conn = conn
        self.cur = cur
        self.DB_STAGING_SCHEMA = "staging"
        self.DB_STAGING_TABLE_NAME = "stgNDVIData"
        self.url="https://vedas.sac.gov.in/krishi/dashboard/index.html"
        self.browser=None

    @commonUtills.handleException(log)
    def __initializeBroswer(self):
        log.info("Initializing browser")
        driver_path = '/home/otsietladm/chromedriver'
        chrome_options = ChromeOptions()
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument("--disable-gpu")  
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        self.browser = Chrome(service=Service(driver_path),options=chrome_options)
        log.info("Browser Initialized")
        return {"success":True}

    @commonUtills.handleException(log)
    def __getData(self,level,max_date):
        self.browser.get(self.url)
        Select(self.browser.find_element(By.XPATH,"//span[text()='Level:']/../select")).select_by_visible_text(level)
        Select(self.browser.find_element(By.XPATH,"//span[text()='Parameter:']/../select")).select_by_visible_text('NDVI')
        time.sleep(5)
        select_date=Select(self.browser.find_element(By.XPATH,"//span[text()='Date:']/../span[2]/select"))
        dates=[x.text for x in select_date.options][-5:]
        ind=dates.index(str(max_date))+1
        if not dates[ind:]:
            log.info("No Dates To Generate Data")
            return {"success":False,"message":"No Dates To Generate Date"}
        
        master_df=pd.DataFrame()
        for date in dates[ind:]:
            log.info(f"Fetching data for level {level} and date {date} ")
            select_date.select_by_visible_text(date)
            time.sleep(2)
            self.browser.find_element(By.XPATH,"//div[@class='counter']").click()
            index_options=Select(self.browser.find_element(By.XPATH,"//div[@class='modal-export']/select"))
            for index in [x.text for x in index_options.options if not "(0)" in x.text]:
                index_options.select_by_visible_text(index)
                time.sleep(2)
                table=self.browser.find_element(By.XPATH,"//table[@id='tabledata']").get_attribute("outerHTML")
                df=pd.read_html(table)[0]
                df["Date"]=date
                master_df=pd.concat([master_df,df],ignore_index=True)
            master_df["location_type"]=level
            master_df['request_date'] = datetime.now().date().strftime('%Y-%m-%d')
            if level.lower()=="tehsil":
                master_df=master_df[~((master_df["Tehsil"]=="None") | (master_df["Tehsil"].isna()))]
            log.info(f"Data fetched for level {level} and date {date} ")
            self.browser.find_element(By.XPATH,"//span[@class='close']").click()
        
        return {"success":True,"data":master_df}

    @commonUtills.handleException(log)
    def __getMaxDatesMaping(self):
        log.info("Fetching max date from db.")
        query="""SELECT
                    (SELECT Date(MAX("date")) FROM staging."stgNDVIData"  where location_type ='Tehsil') AS "Tehsil",
                    (SELECT Date(MAX("date")) FROM staging."stgNDVIData"  where location_type ='District') AS "District"
                        """
        df=pd.read_sql_query(query,self.engine)
        max_dates=df.to_dict("records")[0]
        # print(max_dates)#needed to be commented
        # test_date=datetime.strptime("2023-09-05",'%Y-%m-%d').date()#needed to be commented
        # max_dates={'Tehsil': test_date, 'District':test_date}#needed to be commented
        log.info("Fetched max date from db.")
        return {"success":True,"max_date_maping":max_dates}
    
    @commonUtills.handleException(log)
    def __loadDataToStaging(self, df):
        log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
        log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
        self.conn.commit()
        return {'success' : True}
    
    def _processNdviData(self):
        init_browser_resp=self.__initializeBroswer()
        if not init_browser_resp["success"]:
            return init_browser_resp
        get_max_date_maping_resp=self.__getMaxDatesMaping()
        if not get_max_date_maping_resp["success"]:
            return get_max_date_maping_resp
        max_date_maping=get_max_date_maping_resp["max_date_maping"]

        for level in max_date_maping:
            get_data_resp=self.__getData(level=level,max_date=max_date_maping[level])
            if not get_data_resp["success"]:
                return get_data_resp
            data=get_data_resp["data"]

            formate_column_resp=commonUtills().formatColumnNames(df=data)

            if not formate_column_resp["success"]:
                return formate_column_resp
            
            final_df=formate_column_resp["data"]
            # final_df.to_excel(f"ndvi_{level}.xlsx",index=False)
            # # sys.exit()
            load_to_staging_resp=self.__loadDataToStaging(final_df)
            if not load_to_staging_resp["success"]:
                return load_to_staging_resp
            
        return {"success":True}

def pipeline_handler(connection,connection_type):
    log_file = f"ndvi{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))

    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        log.critical(connect_to_db_resp["message"])
        return {'success' : False, 'message' : connect_to_db_resp['message']}

    log.info(f"ndviETL {connection_type} ETL job started at {start_time_stamp}")
    resp = ndviETL(engine,conn,cur)._processNdviData()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))

    if resp['success']:
        log.info(f"ndviETL ETL job ended at {end_timestamp}")
        subject = f'INFO : ndviETL ETL job status'
        body = f'ndviETL ETL job succeeded. Please check the attached logs for details.'
    else:
        log.critical(f"ndviETL ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : ndviETL ETL job status'
        body = f"ndviETL ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of  ndviETL{connection_type}ETL is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS[1:]:
    pipeline_handler(dbClients(client).connectToDb,client.title())

# start_time = time.time()
# pipeline_handler(dbClients("DEV").connectToDb,"DEV".title())

