
# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import pytz
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import requests
from etl_utils.utils import dbClients,emailClient,commonUtills
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

class faoETL():
    def __init__(self,engine,conn,cur):
        self.engine = engine
        self.conn = conn
        self.cur =cur

    def years(self):
        try:
            max_year = int(pd.read_sql('''select max("Year") from staging."stgFAO" ''',self.conn).values[0])+1
            years = [str(year) for year in range(int(max_year),int(datetime.now().year)+1)]
            if not years:
                return {'success':False,'message':'Data is upto date'}
            else:
                return {'success':True,'years':years}
        except Exception as e:
            log.critical('Error in '+self.years.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.years.__name__+f': {e}'}

    def extractData(self,years):
        try:
            total_df = pd.DataFrame()
            for year in years:
                log.info(f'Extracting data for {year}')
                url = f"https://fenixservices.fao.org/faostat/api/v1/en/data/QCL?area=2,3,4,7,8,9,1,10,11,52,12,13,16,14,57,255,15,23,53,18,19,80,20,21,26,27,233,29,35,115,32,33,37,39,40,351,96,128,214,41,44,45,46,47,48,98,49,50,167,51,107,116,250,54,72,55,56,58,59,60,61,178,63,209,238,62,64,66,67,68,69,70,74,75,73,79,81,84,86,87,89,90,175,91,93,95,97,99,100,101,102,103,104,105,106,109,110,112,108,114,83,118,113,120,119,121,122,123,124,126,256,129,130,131,132,133,134,127,135,136,137,138,145,141,273,143,144,28,147,148,149,150,153,156,157,158,159,160,154,162,221,165,299,166,168,169,170,171,173,174,177,179,117,146,183,185,184,182,188,189,191,244,193,194,195,272,186,196,197,200,199,198,25,201,202,277,203,38,276,206,207,210,211,212,208,216,176,217,218,219,220,222,213,227,223,228,226,230,225,229,215,231,234,235,155,236,237,249,248,251,181&area_cs=M49&element=2312,2510,2413&item=809,800,221,711,515,526,226,366,367,572,839,203,486,44,176,51,552,216,181,420,89,358,101,568,426,217,591,378,125,265,393,108,531,220,191,459,689,401,693,698,661,252,249,656,813,767,329,331,195,554,QD>,QC>,397,550,577,149,399,569,773,94,720,549,560,446,406,675,244,242,225,336,677,277,780,778,310,311,263,782,592,224,407,497,201,372,333,461,210,56,571,1242,671,299,79,103,165,449,292,836,702,75,334,60,258,290,254,430,261,260,403,402,490,414,558,512,821,619,234,339,542,211,723,541,161,603,463,256,257,600,534,521,187,417,687,748,587,197,574,223,489,536,507,296,116,394,754,523,92,788,270,271,547,162,27,71,280,281,328,289,789,83,530,237,236,373,544,423,157,156,267,268,122,305,495,136,667,388,97,777,275,826,692,205,222,567,15,564,137,135&item_cs=CPC&year={year}&show_codes=true&show_unit=true&show_flags=true&show_notes=true&null_values=false&datasource=DB4&output_type=objects"
                payload={}
                headers = {}
                response = requests.request("GET", url, headers=headers, data=payload)
                df = pd.DataFrame.from_records(response.json()['data'])
                if df.empty:
                    log.info(f"No Data found for {year}")
                    continue
                else:
                    total_df = pd.concat([total_df,df],ignore_index=True)
                    log.info(f'Data extracted for {year}')
            if total_df.empty:
                log.info('No latest data avaliable')
                return {'success':False,'message':'No latest data avaliable'}
            else:
                return {'success' : True, 'data' : total_df}
        except Exception as e:
            log.critical('Error in '+self.extractData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.extractData.__name__+f': {e}'}
        

    def loadDataToReporting(self):
        try:
            log.info(f'Loading latest data into reporting."RptFAO" ')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'FAO' and "SeqNo" = 2 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Latest data loaded into reporting."RptFAO" ')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
        
    def loadDataToMIP(self):
        try:
            log.info(f'Loading latest data into reporting.RptMIP')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'FAO' and "SeqNo" = 3 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Latest data loaded into reporting.RptMIP')
            self.conn.commit()

            log.info(f'Loading latest world area and production data into reporting.RptMIP')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'FAO' and "SeqNo" = 4 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Loaded latest area and production data into reporting.RptMIP')
            self.conn.commit()

            log.info(f'Loading latest world yield data into reporting.RptMIP')
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" em where "DataSource" = 'FAO' and "SeqNo" = 5 """)
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(f'''{querytext}''')
            self.conn.commit() 
            log.info(f'Loaded latest yield data into reporting.RptMIP')
            self.conn.commit()
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToMIP.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToMIP.__name__+f': {e}'}
            
        
    def startFaoETL(self):
        try:
            year_resp = self.years()
            if not year_resp['success']:
                return {'success':False,'message':year_resp['message']}
            years = year_resp['years']
            extract_resp = self.extractData(years)
            if not extract_resp['success']:
                return {'success':False,'message':extract_resp['message']}
            # load_data_repor_resp = self.loadDataToReporting()
            # if not load_data_repor_resp['success']:
            #     return {'success' : False, 'message' :load_data_repor_resp['message']}
            # load_data_to_mip_resp = self.loadDataToMIP()
            # if not load_data_to_mip_resp['success']:
            #     return {'success' : False, 'message' :load_data_to_mip_resp['message']}
            else:
                return {'success':True}
        except Exception as e:
            log.critical('Error in '+self.startFaoETL.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.startFaoETL.__name__+f': {e}'}
        
def pipeline_handler(connection,connection_type):
    log_file = f"fao{connection_type}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"FAO ETL job triggered at {start_time_stamp}")
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp=faoETL(engine,conn,cur).startFaoETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type}  FAO ETL job ended at {end_timestamp}")
        subject = f"INFO :{connection_type} FAO ETL job status"
        body = f"{connection_type}  FAO ETL job succeeded. Please check the attached logs for details."
    else:
        log.critical(f"{connection_type} FAO ETL job ended at {end_timestamp}")
        subject = f"CRITICAL : {connection_type} FAO ETL job status"
        body = f"{connection_type} FAO ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of FAO{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)

    # emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    # os.remove(log_file)
    conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())

