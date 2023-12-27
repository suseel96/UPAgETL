# ----------------------------------------------------------------

#                 Pillar-2 Data Refresh ETL JOB

# -----------------------------------------------------------------


# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import pandas as pd
import numpy as np
import pytz
from dotenv import load_dotenv
from etl_utils.utils import emailClient,dbClients,commonUtills
import logging
from datetime import timedelta, datetime
import requests
from datetime import date
import time

# LOGGING CONFIGURATION
log = logging.getLogger()

class refreshPillar2Data:
    def __init__(self,cur,conn,engine):
        self.cur = cur
        self.conn = conn
        self.engine = engine
    def refreshCWWG(self):
        logging.info('Refreshing data for source : CWWG')
        try:
            self.cur.execute(''' CALL upag_p2.sp_cwwg() ''')
            logging.info('Data refreshed for source : CWWG')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : CWWG. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : CWWG. Error: {e}'} 
    def refreshDAFW(self):
        logging.info('Refreshing data for source : DAFW')
        try:
            self.cur.execute(''' CALL upag_p2.sp_dafw() ''')
            logging.info('Data refreshed for source : DAFW')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : DAFW. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : DAFW. Error: {e}'} 
    def refreshDCS(self):
        logging.info('Refreshing data for source : DCS')
        try:
            self.cur.execute(''' CALL upag_p2.sp_dcs() ''')
            logging.info('Data refreshed for source : DCS')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : DCS. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : DCS. Error: {e}'} 
    def refreshFarmersSurvey(self):
        logging.info('Refreshing data for source : Farmers Survey')
        try:
            self.cur.execute(''' CALL upag_p2.sp_farmersurvey() ''')
            logging.info('Data refreshed for source : Farmers Survey')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : Farmers Survey. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : Farmers Survey. Error: {e}'} 
    def refreshGCESCCE(self):
        logging.info('Refreshing data for source : GCES CCE')
        try:
            self.cur.execute(''' CALL upag_p2.sp_gcescce() ''')
            logging.info('Data refreshed for source : GCES CCE')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : GCES CCE. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : GCES CCE. Error: {e}'} 
    def refreshIEG(self):
        logging.info('Refreshing data for source : IEG')
        try:
            self.cur.execute(''' CALL upag_p2.sp_ieg() ''')
            logging.info('Data refreshed for source : IEG')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : IEG. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : IEG. Error: {e}'} 
    def refreshMNCFC(self):
        logging.info('Refreshing data for source : MNCFC')
        try:
            self.cur.execute(''' CALL upag_p2.sp_mncfc() ''')
            logging.info('Data refreshed for source : MNCFC')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : MNCFC. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : MNCFC. Error: {e}'} 
    def refreshNSSO(self):
        logging.info('Refreshing data for source : NSSO')
        try:
            self.cur.execute(''' CALL upag_p2.sp_nsso() ''')
            logging.info('Data refreshed for source : NSSO')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : NSSO. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : NSSO. Error: {e}'} 
    def refreshPmfbyAyty(self):
        logging.info('Refreshing data for source : PMFBY AYTY')
        try:
            self.cur.execute(''' CALL upag_p2.sp_pmfbyayty() ''')
            logging.info('Data refreshed for source : PMFBY AYTY')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : PMFBY AYTY. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : PMFBY AYTY. Error: {e}'} 
    def refreshPmfbyCCE(self):
        logging.info('Refreshing data for source : PMFBY CCE')
        try:
            self.cur.execute(''' CALL upag_p2.sp_pmfbycce() ''')
            logging.info('Data refreshed for source : PMFBY CCE')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : PMFBY CCE. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : PMFBY CCE. Error: {e}'}
    def refreshSammunati(self):
        logging.info('Refreshing data for source : Sammunati')
        try:
            self.cur.execute(''' CALL upag_p2.sp_sammunati() ''')
            logging.info('Data refreshed for source : Sammunati')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : Sammunati. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : Sammunati. Error: {e}'} 
    def refreshStateData(self):
        logging.info('Refreshing data for source : State data')
        try:
            self.cur.execute(''' CALL upag_p2.sp_statedata() ''')
            logging.info('Data refreshed for source : State data')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh data for source : State data. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh data for source : State data. Error: {e}'} 
        
    def refreshSubdistrictWeights(self):
        logging.info('Refreshing weights for subdistricts')
        try:
            self.cur.execute('''select upag_p2.fn_set_dcs_gces_weigted_area_subdistrict()''')
            logging.info('Weights refreshed for subdistricts')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh weights for subdistricts. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh weights for subdistricts. Error: {e}'}
        
    def refreshDistrictWeights(self):
        logging.info('Refreshing weights for districts')
        try:
            self.cur.execute('''select upag_p2.fn_set_dcs_gces_weigted_area_district()''')
            logging.info('Weights refreshed for districts')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh weights for districts. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh weights for districts. Error: {e}'}
    
    def refreshStateWeights(self):
        logging.info('Refreshing weights for states')
        try:
            self.cur.execute('''select upag_p2.fn_set_dcs_gces_weigted_area_state()''')
            logging.info('Weights refreshed for states')
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh weights for states. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh weights for states. Error: {e}'} 
        
    def refreshData(self):
        try:
            self.refreshDCS()
            self.conn.commit()
            self.refreshGCESCCE()
            self.conn.commit()
            self.refreshDAFW()
            self.conn.commit()
            self.refreshSubdistrictWeights()
            self.conn.commit()
            self.refreshDistrictWeights()
            self.conn.commit()
            self.refreshStateWeights()
            self.conn.commit()
            self.refreshCWWG()
            self.conn.commit()
            self.refreshFarmersSurvey()
            self.conn.commit()
            self.refreshIEG()
            self.conn.commit()
            self.refreshMNCFC()
            self.conn.commit()
            self.refreshNSSO()
            self.conn.commit()
            self.refreshPmfbyAyty()
            self.conn.commit()
            self.refreshPmfbyCCE()
            self.conn.commit()
            self.refreshSammunati()
            self.conn.commit()
            self.refreshStateData()
            self.conn.commit()
            return {'success':True}
        except Exception as e:
            logging.critical(f'Unable to refresh Pillar-2 data. Error: {e}')
            return {'success':False, 'message' : f'Unable to refresh Pillar-2 data. Error: {e}'}         

def pipeline_handler(connection,connection_type):
    log_file = f"pillar2datarefresh_{(connection_type).lower()}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"Pillar 2 data refresh job started at {start_time_stamp}")
    connect_db_resp = connection()
    if not connect_db_resp['success']:
        return {'success' : False, 'message' : connect_db_resp['message']}
    engine = connect_db_resp['engine']
    conn = connect_db_resp['conn']
    cur = connect_db_resp['cur']
    resp = refreshPillar2Data(cur,conn,engine).refreshData()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} Pillar 2 data refresh job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} Pillar 2 data refresh job status"
        body = f"{connection_type} Pillar 2 data refresh job succeeded. Please check the attached logs for details."
    else:
        log.critical(f"{connection_type} Pillar 2 data refresh job ended at {end_timestamp}")
        subject = f"CRITICAL : {connection_type} Pillar 2 data refresh job status"
        body = f"{connection_type} Pillar 2 data refresh job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of Pillar 2 data refresh job{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
# DB_CLIENTS = ['STAGING','PRODUCTION']
# DB_CLIENTS = ['DEV']
# DB_CLIENTS = ['STAGING']
DB_CLIENTS = ['PRODUCTION']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())