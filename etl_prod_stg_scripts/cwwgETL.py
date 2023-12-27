# ----------------------------------------------------------------

#                 CWWG ETL JOB ON EVERY MONDAY

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

class cwwgETL:
    def __init__(self,cur,conn,engine):
        self.cwwg_url = 'https://www.nfsm.gov.in/services/areacoveragereport.asmx/GetAllCropsAreacoveragerecord'
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME = 'stgCWWGData'
        self.cur = cur
        self.conn = conn
        self.engine = engine
    def checkDataAvailabilityAndExtract(self):

        logging.info('Checking data availability')
        try:
            param = {'dateddmmyy':'0'}
            page_resp = requests.get(self.cwwg_url, param)
            if 'data' in page_resp.json().keys():
                state_data = pd.DataFrame.from_records(page_resp.json()['data'])
                logging.info('Data received in json response')
                return {'success':True, 'data': state_data}
            else:
                logging.critical('No data received in json response.')
                return {'success':False, 'message':'No data received in json response.'}
        except Exception as e:
            logging.critical(f'Unable to check data availability. Error: {e}')
            return {'success':False, 'message' : f'Unable to check data availability. Error: {e}'} 
    
    def TransformData(self, df):
        try:
            logging.info('Transforming the data.')
            col_list = df.columns.tolist()
            cols = ['Cropname' if 'Cropname' in x else 'Crop_Code' if 'Crop_Code' in x
                    else 'CurrentArea' if 'CurrentArea' in x
                    else 'PreviousArea1' if 'PreviousArea1' in x
                    else 'PreviousArea2' if 'PreviousArea2' in x
                    else 'Difference_Area1' if 'Difference_Area1' in x
                    else 'Difference_Area2' if 'Difference_Area2' in x else x for x in col_list]
            df.columns = cols
            df.set_index(['Slno', 'Division', 'Schemecode', 'Schemename', 'Statecode',
            'Statename', 'CurrentYear', 'LastYear', 'Asondate'], inplace=True)
            main_df = pd.DataFrame()
            for i in range(0,(len(cols)), 7)[:-2]:
                main_df = pd.concat([main_df,df.iloc[:,i:i+7].reset_index()], ignore_index=True)
            main_df = main_df.replace(r'^\s*$', np.nan, regex=True)
            main_df['Asondate'] = pd.to_datetime(main_df['Asondate'], dayfirst=True)
            main_df.insert(9,'Season',main_df['Asondate'].apply(lambda x: 'Summer' if 2<=x.month<=5 else 'Kharif' if 6<=x.month<=9 else 'Rabi'))
            main_df.drop(['Slno', 'Division', 'Schemecode', 'Schemename', 'Statecode'], axis=1, inplace=True)
            main_df.loc[main_df['Cropname'] == 'Pea', 'Cropname'] = 'Peas'
            main_df.loc[main_df['Cropname'] == 'Rapeseed and Mustard', 'Cropname'] = 'Rapeseed & Mustard'
            main_df['Crop year'] = main_df['Asondate'].apply(lambda x: str(int(x.year)-1)+' - '+str(x.year)[-2:] if 1<=x.month<=5 
                                else str(x.year)+' - '+str(int(x.year)+1)[-2:])
            main_df['Calendar Date Key'] =  main_df['Crop year'].apply(lambda x: str(x)[:4]+'0701')
            main_df[['CurrentArea', 'PreviousArea1','Difference_Area1', 'PreviousArea2',
                'Difference_Area2']] = main_df[['CurrentArea', 'PreviousArea1', 'Difference_Area1', 'PreviousArea2', 
                                                'Difference_Area2']].astype('float')*100
            main_df[['CurrentArea UOM', 'PreviousArea1 UOM',
            'Difference_Area1 UOM', 'PreviousArea2 UOM', 'Difference_Area2 UOM']]='hectares'
            main_df[['CurrentArea Scaling', 'PreviousArea1 Scaling',
                'Difference_Area1 Scaling', 'PreviousArea2 Scaling', 'Difference_Area2 Scaling']]=1000
            main_df['Client Key']=1
            main_df['Company Key']='MOA'
            main_df['Request ID']=np.nan
            main_df['User Agency Key']='CWWG'
            main_df['District Name']=np.nan
            main_df['Fiscal Year Variant Key']='C2'
            main_df = main_df[['Client Key', 'Company Key', 'Request ID', 'User Agency Key', 'Statename', 'District Name', 'Fiscal Year Variant Key', 
                'Calendar Date Key', 'CurrentYear', 'LastYear', 'Asondate', 'Season', 'Cropname', 'Crop_Code', 'CurrentArea', 
                'CurrentArea UOM', 'CurrentArea Scaling', 'PreviousArea1', 'PreviousArea1 UOM', 'PreviousArea1 Scaling',
                'Difference_Area1', 'Difference_Area1 UOM', 'Difference_Area1 Scaling', 'PreviousArea2', 'PreviousArea2 UOM',
                'PreviousArea2 Scaling', 'Difference_Area2', 'Difference_Area2 UOM','Difference_Area2 Scaling','Crop year']]
            main_df.columns = main_df.columns.str.replace('_','')
            main_df.columns = main_df.columns.str.replace('Area',' Area')
            main_df.rename(columns={'Cropname':'Crop Key', 'Statename':'State Name'}, inplace=True)
            main_df['State Name'] = main_df['State Name'].str.lstrip().str.rstrip()
            date_str = str(main_df['Asondate'].unique()[0])[:10]
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            year, week_number, weekday = date_obj.isocalendar()
            main_df['week_num'] = week_number
            logging.info('Transfomations completed on the data.')
            return {'success' : True, 'data' : main_df}
        except Exception as e:
            logging.critical(f'Transformations failed. Error: {e}')
            return {'success' : False, 'message' : f'Transformations failed. Error: {e}'}
  
    def standardizeStateNames(self, df):
        try:
            logging.info('Standardizing state names and adding state key. Reading state names master  from db.')
            master = pd.read_sql('''SELECT "StateName","Synonyms","StateCode" FROM staging.state_name_code_lookup
                                    WHERE "Source" = 'LGD' ''', self.conn)
            master_list = master.to_dict('records')
            def master_lookup(row):
                for state in master_list:
                    if row["State Name"].lower() in state['Synonyms'].lower():
                        row['State Name']=state["StateName"]
                        row["State Key"]=int(state["StateCode"])
                return row
            df= df.apply(master_lookup,axis=1)
            if df['State Name'].isnull().sum(axis=0) > 0 and df['State Key'].isnull().sum(axis=0) > 0:
                logging.info('State names not standardized.')
                return {'success':False, 'message' : 'State names not standardized'}
            else:
                logging.info('Standardizing state names successful.')
                return {'success':True, 'data' : df}
        except Exception as e:
            logging.critical(f'Error standardizing state names. Error : {e}')
            return {'success':False, 'message' : f'Error standardizing state names. Error : {e}'}

    def loadDataToDb(self, df):
        try:
            logging.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}.')
            df.to_sql(self.DB_STAGING_TABLE_NAME, self.engine, schema=self.DB_STAGING_SCHEMA, if_exists="append", index=False)
            self.conn.commit()
            self.cur.execute(f''' select count(distinct "Crop Key") from {self.DB_STAGING_SCHEMA}."{self.DB_STAGING_TABLE_NAME}" 
                            where "Current Area" is not null and "Crop Key" != 'Lathyrus' ''')
            res = self.cur.fetchall()
            stg_crop_count = int(res[0][0])
            self.cur.execute(f''' select count(distinct "State Name") from {self.DB_STAGING_SCHEMA}."{self.DB_STAGING_TABLE_NAME}"
                            where "Current Area" is not null''')
            res = self.cur.fetchall()
            stg_state_count = int(res[0][0])
            logging.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{self.DB_STAGING_TABLE_NAME}.')
            return {'success' : True, 'stgCropCount' :stg_crop_count, 
                    'stgStateCount' : stg_state_count, 'message' : 'Data loaded into the database'}
        except Exception as e:
            logging.info('Database connection closed.')
            return {'success' : False, 'message' : f'Unable to load data to database. Error: {e}'}
            
    def loadDataIntoDataTriangulation(self):
        try:
            logging.info('Loading data into data triangulation.')
            self.cur.execute(''' CALL upag.sp_cwwg() ''')
            res = self.cur.fetchall()
            res_vars = res[0]
            triangulation_crop_count = int(res_vars[1])
            triangulation_state_count = int(res_vars[2])
            self.conn.commit()
            logging.info(f'''Data loaded into data triangulation. ''')
            return {'success' : True, 'triangulationCropCount' :triangulation_crop_count, 
                    'triangulationStateCount' : triangulation_state_count, 
                    'message' : f'Data loaded into data triangulation. Distinct crops : {triangulation_crop_count}, Distinct states : {triangulation_state_count}'}
        except Exception as e:
            logging.critical(f'Unable to load data into data triangulation. Error: {e}')
            return {'success' : False, 'message' : f'Unable to load data to data triangulation. Error: {e}'}

            
    def validations(self, stg_crop_count, stg_state_count, triangulation_crop_count, triangulation_state_count):
        try:
            logging.info('Validating data.')
            if ((stg_crop_count == triangulation_crop_count) and (stg_state_count == triangulation_state_count)):
                logging.info(f'Data validated. Distinct crops : {stg_crop_count}, Distinct states : {stg_state_count}')
                return {'success' : True, 
                        'message' : f'''Data validated. 
                                    Distinct crops : {stg_crop_count}, 
                                    Distinct states : {stg_state_count}'''}
            else:
                logging.critical(f'''Data validation failed. 
                                 Distinct crops in staging : {stg_crop_count}
                                 Distinct states in staging : {stg_state_count}
                                 Distinct crops in triangulation : {triangulation_crop_count},
                                 Distinct states in triangulation : {triangulation_state_count}''')
                return {'success' : False, 
                        'message' : f''' Data validation failed. 
                           Distinct crops in staging : {stg_crop_count}', 
                           Distinct states in staging : {stg_state_count},
                           Distinct crops in triangulation : {triangulation_crop_count}', 
                           Distinct states in triangulation : {triangulation_state_count}'''}
        except Exception as e:
            logging.critical(f'Unable to validate data. Error : {e}')
            return {'success' : False, 'message' : f'Unable to validate data. Error : {e}'}
        
    def startCwwgETL(self):
        try:
            
            check_data_availability_resp = self.checkDataAvailabilityAndExtract()
            if not check_data_availability_resp['success']:
                return {'success' : False, 'message' : check_data_availability_resp['message']}
            state_data = check_data_availability_resp['data']
            transform_data_resp = cwwgETL(self.cur,self.conn,self.engine).TransformData(state_data)
            if not transform_data_resp['success']:
                return {'success' : False, 'message' : transform_data_resp['message']}
            transformed_data = transform_data_resp['data']
            
            # cur.execute(''' select to_char(max("Asondate")::date, 'DD-MM-YYYY')  
            #             from staging."stgCWWGData"''')
            # res = cur.fetchone()
            # db_date = date(int(res[0].split('-')[2]), int(res[0].split('-')[1]), int(res[0].split('-')[0]))
            # data_date = pd.to_datetime(transformed_data['Asondate'], dayfirst=True).apply(lambda x:str(x).split()[0])[0]
            # data_date = date(int(data_date.split('-')[0]), int(data_date.split('-')[1]), int(data_date.split('-')[2]))
            # if data_date <= db_date:
            #     return {'success' : False, 'message' : f'Data already exists for {transformed_data["Asondate"].unique()[0]}'}
            standardize_states_resp = self.standardizeStateNames( transformed_data)
            if not standardize_states_resp['success']:
                return {'success' : False, 'message' : standardize_states_resp['message']}
            transformed_data = standardize_states_resp['data']
            load_data_resp = self.loadDataToDb(transformed_data)
            if not load_data_resp['success']:
                return {'success' : False, 'message' : load_data_resp['message']}
            stg_crop_count = load_data_resp['stgCropCount'] 
            stg_state_count = load_data_resp['stgStateCount']
            triangulation_resp = self.loadDataIntoDataTriangulation()
            if not triangulation_resp['success']:
                return {'success' : False, 'message' : triangulation_resp['message']}
            triangulation_crop_count = triangulation_resp['triangulationCropCount']
            triangulation_state_count = triangulation_resp['triangulationStateCount']
            validations_resp = self.validations(stg_crop_count, stg_state_count, triangulation_crop_count, 
                                                triangulation_state_count)
            if validations_resp['success']:
                return {'success' : True, 'message' : validations_resp['message']}
            else:
                return {'success' : False, 'message' : validations_resp['message']}
            
        except Exception as e:
            log.critical('Error in '+self.startCwwgETL.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.startCwwgETL.__name__+f': {e}'}
    
def pipeline_handler(connection,connection_type):
    log_file = f"cwwg{(connection_type).lower()}etl.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"CWWG ETL job started at {start_time_stamp}")
    connect_db_resp = connection()
    if not connect_db_resp['success']:
        return {'success' : False, 'message' : connect_db_resp['message']}
    engine = connect_db_resp['engine']
    conn = connect_db_resp['conn']
    cur = connect_db_resp['cur']
    resp = cwwgETL(cur,conn,engine).startCwwgETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} CWWG ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} CWWG ETL job status"
        body = f"{connection_type} CWWG ETL job succeeded. Please check the attached logs for details."
    else:
        log.critical(f"{connection_type} CWWG ETL job ended at {end_timestamp}")
        subject = f"CRITICAL : {connection_type} CWWG ETL job status"
        body = f"{connection_type} CWWG ETL job failed. Error : {resp['message']} Please check the attached logs for details."
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of cwwg{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
DB_CLIENTS = ['STAGING','PRODUCTION']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())