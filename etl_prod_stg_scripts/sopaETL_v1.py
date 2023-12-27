# ----------------------------------------------------------------

#                 SOPA DAILY ETL JOB

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
import re

#LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class sopaPricesETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.commodity = {'Soya oil':'Soybean Solvent Oil', 'RBD Soya oil': 'Soybean Refined Oil', 'Soybean (Meal/Cake)':'Soybean meal'}
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'sopaoilprices'
        self.DB_STAGING_TABLE_NAME1 = 'sopamealprices'
    def generateOilDates(self):
        try:
            df = pd.read_sql('''
                        select commodity ,max("date") "Date" from staging.sopaoilprices 
                        group by commodity ''' ,self.conn)
            solvent_oil_date = df[df['commodity'] == 'Soybean Solvent Oil']['Date'].values[0]
            solvent_oil_dates = pd.date_range(start = solvent_oil_date + timedelta(days = 1 ) , end = datetime.now().date())
            solvent_oil_dates = [datetime.strftime(date,'%Y-%m-%d') for date in solvent_oil_dates]
            
            refined_oil_date = df[df['commodity'] == 'Soybean Refined Oil']['Date'].values[0]
            refined_oil_dates = pd.date_range(start = refined_oil_date + timedelta(days = 1 ) , end = datetime.now().date())
            refined_oil_dates = [datetime.strftime(date,'%Y-%m-%d') for date in refined_oil_dates]
            return {'success':True, 'solvent_oil_dates':solvent_oil_dates,'refined_oil_dates':refined_oil_dates}
        except Exception as e:
            log.critical('Error in '+self.generateOilDates.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.generateOilDates.__name__+f': {e}'}
        
    def scrapeSolventOilData(self,dates):
        try:
            log.info("Started Scraping Solvent Oil Data")
            driver_path = '/home/otsietladm/chromedriver'
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (often needed in headless mode)
            chrome_options.add_argument("--no-sandbox")
            browser = Chrome(service=Service(driver_path), options=chrome_options)
            final_df = pd.DataFrame()
            for date in dates:
                browser.get(f'https://www.sopa.org/daily-rates/soybean-oil/?solvoil=solvent-oil&search_type=search_by_date&single_date={date}&starting_year_value=&ending_year_value=&station=Indore&submit=Search')
                table = browser.find_element(By.TAG_NAME,'table').get_attribute('outerHTML')
                try:
                    df = pd.read_html(table)[0]
                    df['price'] = df[df.columns[1]].apply(lambda x:sum([float(ele) for ele in (x.split('Rs.')[-1].split('/')[0].split('-'))])/len([float(ele) for ele in (x.split('Rs.')[-1].split('/')[0].split('-'))]))
                    df['unit'] = 'Rs/10 Kg'
                    df['Commodity'] = 'Soybean Solvent Oil'
                    df['Date'] = df['Date'].apply(lambda x:datetime.strptime(x, "%d %B, %Y"))
                    df.rename(columns= {df.columns[1]:'pricerange'},inplace=True)
                    log.info(f"Data fetched for {date} for Soybean Solvent Oil ")
                    final_df = pd.concat([final_df,df],ignore_index=True)
                except:
                    no_record = browser.find_element(By.XPATH,"//h3[contains(text(), 'No Record Found')]")
                    if no_record.is_displayed:
                            log.info(f'No records found for {date}')
                            continue
            final_df = final_df.drop_duplicates()
            final_df['request_date'] = datetime.now().date()
            log.info("Successfully Scrapped Data for Soyabean Solvent Oil")
            return {"success":True,"data":final_df}
        except Exception as e:
            log.critical('Error in '+self.scrapeSolventOilData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.scrapeSolventOilData.__name__+f': {e}'}
        
    def scrapeRefinedOilData(self,dates):
        try:
            log.info("Started Scraping Refined Oil Data")
            driver_path = '/home/otsietladm/chromedriver'
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (often needed in headless mode)
            chrome_options.add_argument("--no-sandbox")
            browser = Chrome(service=Service(driver_path), options=chrome_options)
            final_df = pd.DataFrame()
            for date in dates:
                browser.get(f'https://www.sopa.org/daily-rates/refined-oil/?solvoil=refined-oil&search_type=search_by_date&single_date={date}&starting_year_value=&ending_year_value=&station=Indore&submit=Search')
                table = browser.find_element(By.TAG_NAME,'table').get_attribute('outerHTML')
                try:
                    df = pd.read_html(table)[0]
                    df['price'] = df[df.columns[1]].apply(lambda x:sum([float(ele) for ele in (x.split('Rs.')[-1].split('/')[0].split('-'))])/len([float(ele) for ele in (x.split('Rs.')[-1].split('/')[0].split('-'))]))
                    df['unit'] = 'Rs/10 Kg'
                    df['Commodity'] = 'Soybean Refined Oil'
                    df['Date'] = df['Date'].apply(lambda x:datetime.strptime(x, "%d %B, %Y"))
                    df.rename(columns= {df.columns[1]:'pricerange'},inplace=True)
                    log.info(f"Data fetched for {date} for Soybean Refined Oil ")
                    final_df = pd.concat([final_df,df],ignore_index=True)
                except:
                    no_record = browser.find_element(By.XPATH,"//h3[contains(text(), 'No Record Found')]")
                    if no_record.is_displayed:
                            log.info(f'No records found for {date}')
                            continue
            final_df = final_df.drop_duplicates()
            final_df['request_date'] = datetime.now().date()
            log.info("Successfully Scrapped Data for Soyabean Refined Oil")
            return {"success":True,"data":final_df}
        except Exception as e:
            log.critical('Error in '+self.scrapeRefinedOilData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.scrapeRefinedOilData.__name__+f': {e}'}
            
    
    
    def generateMealDates(self):
        try:
            df = pd.read_sql('''
                        select max("date") from staging.sopamealprices
                             ''' ,self.conn)
            max_date = df['max'].values[0]
            dates = pd.date_range(start = max_date + timedelta(days = 1 ) , end = datetime.now().date())
            dates = [datetime.strftime(date,'%Y-%m-%d') for date in dates]
            return {'success':True, 'dates':dates}
        except Exception as e:
            log.critical('Error in '+self.generateMealDates.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.generateMealDates.__name__+f': {e}'}
        
    def scrapeMealData(self,dates):
        try:
            log.info("StartedScraping Soyameal Data")
            driver_path = '/home/otsietladm/chromedriver'
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (often needed in headless mode)
            chrome_options.add_argument("--no-sandbox")
            browser = Chrome(service=Service(driver_path), options=chrome_options)
            final_df = pd.DataFrame()
            for date in dates:
                try:
                    browser.get(f'https://www.sopa.org/daily-rates/indore-market/soybean-meal/?search_type=search_by_date&single_date={date}&starting_year_value=&ending_year_value=&station=indore&submit=Search')                    
                    table_el = browser.find_element(by='xpath', value = '/html/body/div[1]/div[2]/div[3]/div[3]/table').get_attribute('outerHTML')
                    data = pd.read_html(table_el)[0]
                    data['Ex-Factory (Indore) Against "H" form Price'] = data['Ex-Factory (Indore) Against "H" form'].apply(lambda x: re.findall(r'(\d+(?:\.\d+)?)', x))
                    data['Ex-Factory (Indore) Against "H" form Price'] = data['Ex-Factory (Indore) Against "H" form Price'].apply(lambda x: [float(i) for i in x if float(i)!=0])
                    data['Ex-Factory (Indore) Against "H" form Price'] = data['Ex-Factory (Indore) Against "H" form Price'].apply(lambda x : sum(x)/len(x) if len(x)>0 else np.nan)
                    data['Date'] = data['Date'].apply(lambda x: datetime.strptime(x,'%d %B, %Y'))
                    data['uom'] = 'Rs/Ton'
                    data['commodity'] = 'Soybean meal'
                    log.info(f'Data fetched for soya meal for {date}')
                    final_df = pd.concat([final_df,data],ignore_index=True)
                except:
                    no_record = browser.find_element(By.XPATH,"//h3[contains(text(), 'No Record Found')]")
                    if no_record.is_displayed:
                            log.info(f'No records found for {date}')
                            continue
            final_df = final_df.drop_duplicates()
            final_df['request_date'] = datetime.now().date()
            log.info("Successfully Scrapped Meal Data")
            return {"success":True,"data":final_df}
        except Exception as e:
            log.critical('Error in '+self.scrapeMealData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.scrapeMealData.__name__+f': {e}'}

    def loadDataToStaging(self, solvent_oil_df,refined_oil_df , meal_df):
        try:
            oils_df = pd.concat([solvent_oil_df,refined_oil_df],ignore_index=True)
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            oils_df.to_sql(f'{self.DB_STAGING_TABLE_NAME}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
            log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}')
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME1}')
            meal_df.to_sql(f'{self.DB_STAGING_TABLE_NAME1}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
            log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME1}')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToStaging.__name__+f': {e}'}

    def stagingToReporting(self):
        
        try:
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'SOPA' and "SeqNo" = 1 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Inserted oils data into reporting.sopaprices')

            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'SOPA' and "SeqNo" = 2 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit()
            log.info(f'Inserted meal data into reporting.sopaprices')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.stagingToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.stagingToReporting.__name__+f': {e}'}

    def reportingToMIP(self):
        try:
            for commodity_param in self.commodity:
                self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'SOPA' and "SeqNo" = 3 """)
                querytext= self.cur.fetchall()[0][0]
                self.cur.execute(f'''{querytext}''', {'commodity_param':commodity_param,'commodity':self.commodity[commodity_param]})
                log.info(f'{commodity_param} Data loaded to MIP ')
                self.conn.commit() 
            return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.reportingToMIP.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.reportingToMIP.__name__+f': {e}'}



    def startSopaPricesETL(self):
        try:
            oil_dates_resp = self.generateOilDates()
            if not oil_dates_resp['success']:
                return {'success': False, 'message' : oil_dates_resp['message']}
            solvent_oil_dates = oil_dates_resp['solvent_oil_dates']
            refined_oil_dates = oil_dates_resp['refined_oil_dates']
            
            scrape_soyabean_oil_data_resp = self.scrapeSolventOilData(solvent_oil_dates)
            if not scrape_soyabean_oil_data_resp['success']:
                return {'success': False, 'message' : scrape_soyabean_oil_data_resp['message']}
            
            scrape_soyabean_oil_column_resp = commonUtills().formatColumnNames(scrape_soyabean_oil_data_resp['data'])
            if not scrape_soyabean_oil_column_resp['success']:
                return {'success': False, 'message' : scrape_soyabean_oil_column_resp['message']}
            solvent_oil_df  = scrape_soyabean_oil_column_resp['data']  
            scrape_refined_oil_data_resp = self.scrapeRefinedOilData(refined_oil_dates)
            if not scrape_refined_oil_data_resp['success']:
                return {'success': False, 'message' : scrape_refined_oil_data_resp['message']}
            
            scrape_refined_oil_column_resp = commonUtills().formatColumnNames(scrape_refined_oil_data_resp['data'])
            if not scrape_refined_oil_column_resp['success']:
                return {'success': False, 'message' : scrape_refined_oil_column_resp['message']}
            refined_oil_df  = scrape_refined_oil_column_resp['data']
            meal_dates_resp = self.generateMealDates()
            if not meal_dates_resp['success']:
                return {'success': False, 'message' : meal_dates_resp['message']}
            meal_dates = meal_dates_resp['dates']
            scrape_meal_data_resp = self.scrapeMealData(meal_dates)
            if not scrape_meal_data_resp['success']:
                return {'success': False, 'message' : scrape_meal_data_resp['message']}
            meal_column_resp = commonUtills().formatColumnNames(scrape_meal_data_resp['data'])
            if not meal_column_resp['success']:
                return {'success': False, 'message' : meal_column_resp['message']}
            meal_df = meal_column_resp['data']
            load_data_to_staging_resp = self.loadDataToStaging(solvent_oil_df,refined_oil_df , meal_df)
            if not load_data_to_staging_resp['success']:
                return {'success': False, 'message' : load_data_to_staging_resp['message']}
            
            stg_repor_resp = self.stagingToReporting()
            if not stg_repor_resp['success']:
                return {'success':False,'message':stg_repor_resp['message']}
            repor_mip_resp = self.reportingToMIP()
            if not repor_mip_resp['success']:
                return {'success':False,'message':repor_mip_resp['message']}
            else:
                return {'success': True}
        except Exception as e:
            log.critical('Error in '+self.startSopaPricesETL.__name__+f': {e}')
            return {'success': False, 'message' : 'Error in '+self.startSopaPricesETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"sopaprices{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"SOPA ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp = sopaPricesETL(engine,conn,cur).startSopaPricesETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} SOPA ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} SOPA ETL job status"
        body = f"{connection_type} SOPA ETL job succeeded. Please check the attached logs for details."
    elif 'No Data Available' in  resp['message']:
        conn.close()
        os.remove(log_file)
        os._exit()
    else:
        log.critical(f"{connection_type} SOPA ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {connection_type} SOPA ETL job status'
        body = f"{connection_type} SOPA ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of sopa{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())