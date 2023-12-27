# ----------------------------------------------------------------

#                 DCS DAILY ETL JOB

# -----------------------------------------------------------------

# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import shutil
import pytz
import math
from etl_utils.utils import emailClient, fileUploadUtils, dbClients,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import requests
import time
import uuid
# LOGGING CONFIGURATION
log = logging.getLogger()

# ETL CODE
class dcsETL:
  def __init__(self, engine, conn, cur):
    self.file_path = '/home/otsietladm/dev_file_uploads/dcs-api/pending/'
    self.token_url = "https://34.160.198.182/oauth2/token"
    self.auth_token = "UjV3TV9oeW1PZndSdnVsN1JkUmZqbGh0ZmpzYTprTGxUZmhCcVhjSjBsUGl1RjFvSW5uXzRIQTRh"
    self.seek_api_url = 'https://34.160.198.182/g2p/seek/v1/agristack/seek'
    self.on_seek_api_url = "https://api-dev.upag.gov.in/agristack/on-seek"
    self.tx_id = str(uuid.uuid4())
    self.rf_id = str(uuid.uuid4())
    self.msg_id = str(uuid.uuid4())
    self.page_size = 10000
    current_utc_datetime = datetime.now(pytz.utc)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    ist_datetime = current_utc_datetime.astimezone(ist_timezone)
    self.timestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    self.states_list = [21, 23, 24, 9]
    self.season_list = ['Kharif', 'Rabi', 'Zaid']
    self.engine = engine
    self.conn = conn
    self.cur = cur
  def getToken(self, auth_token):
    payload = 'grant_type=client_credentials'
    headers = {
      'Authorization': f'Basic {auth_token}',
      'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.request("POST", self.token_url, headers=headers, data=payload, verify=False)
    if response.status_code == 200:
      return {'success':True,'access_token':response.json()['access_token']}
    else:
      return {'success':False, 'message': f'Unable to retrieve auth token: {response.text}'}
    
  def readDataFromFile(self, file_name):
    try:
      log.info(f'Reading data from {file_name}')
      with open(file_name, 'r') as data:
        file_data = json.load(data)
      log.info(f'Data read from {file_name} successsfully.')
      return {'success' : True, 'data' : file_data}
    except Exception as e:
      log.critical('Error in '+self.readDataFromFile.__name__+f': {e}')
      return {'success' : False, 'message' : 'Error in '+self.readDataFromFile.__name__+f': {e}'}
  def requestDataInPages(self):
    try:
      page_count_files = os.listdir(self.file_path)
      page_count_files = [self.file_path + file for file in page_count_files]
      for file in page_count_files:
        read_data_from_file_resp = self.readDataFromFile(file)
        data = read_data_from_file_resp['data']
        log.info(''' Requesting data in pages ''')
        if not data["is_validated"]:
          log.critical(f"Payload not validated for tx_id {file.split('/')[-1].split('.json')[0]}")
          return {'success' : False, 'message':'Payload not validated'}
        log.info(f"Payload validated for tx_id {file.split('/')[-1].split('.json')[0]}. Processing data for this transaction.")
        tx_id = file.split('/')[-1].split('.json')[0]
        request_params_resp = self.fetchRequestParams(tx_id)
        state = request_params_resp['state']
        year = request_params_resp['year']
        season = request_params_resp['season']
        if (
            data['validated_object']['payload']['message']['pagination']['page_size'] == 1) and (
            data['validated_object']['payload']['message']['pagination']['page_number'] == 1):
                record_count = data['validated_object']['payload']['message']['pagination']['total_count']
                page_count = math.ceil(record_count/self.page_size)
                for page in range(page_count):
                  dcsETL(self.engine,self.conn,self.cur).requestSeekAPI(year = year, state = state, season = season, 
                                                                        page_size=self.page_size, page_num=page+1)
                  log.info(f"Data requested for tx_id {file.split('/')[-1].split('.json')[0]}.")
        fileUploadUtils().moveProcessedFile('processed', file, upload_timestamp=None)
      return {'success' : True, 'message':f"Data for tx_id {file.split('/')[-1].split('.json')[0]} inserted into db."}
    except Exception as e:
        log.critical('Error in '+self.requestDataInPages.__name__+f': {e}')
        return {'success' : False, 'message' : 'Error in '+self.requestDataInPages.__name__+f': {e}'}
  def processDataInPages(self):
    try:
      data_files = os.listdir(self.file_path)
      data_files = [self.file_path + file for file in data_files]
      for file in data_files:
        read_data_from_file_resp = self.readDataFromFile(file)
        data = read_data_from_file_resp['data']
        log.info(''' Processing data in pages ''')
        if not data["is_validated"]:
          log.critical(f"Payload not validated for tx_id {file.split('/')[-1].split('.json')[0]}")
          return {'success' : False, 'message':'Payload not validated'}
        log.info(f"Payload validated for tx_id {file.split('/')[-1].split('.json')[0]}. Processing data for this transaction.")
        tx_id = file.split('/')[-1].split('.json')[0]
        request_params_resp = self.fetchRequestParams(tx_id)
        state = request_params_resp['state']
        year = request_params_resp['year']
        season = request_params_resp['season']
        total_df = pd.DataFrame()
        for i in data['validated_object']['payload']['message']['search_response']:
          if i['data']['reg_records']:
              df = pd.DataFrame(i['data']['reg_records'])
              total_df = pd.concat([total_df, df], ignore_index=True)
        if total_df.empty:
          continue
        total_df['state_code'] = state
        total_df = total_df[["state_code", "year", "survey_status", "crop_name", "crop_code", "district_name", 
                              "village_name","village_lgd_code", "season", "irrigation_type", "sown_area", "area_unit"]]
        for _, row in total_df.iterrows():
          self.cur.execute(f'''
                          INSERT INTO staging.stgdcsdata (state_code, year , survey_status, crop_name, crop_code, district_name, 
                          village_name, village_lgd_code, season, irrigation_type, sown_area, area_unit)
                          VALUES (%(state_code)s, %(year)s, %(survey_status)s,%(crop_name)s,%(crop_code)s,%(district_name)s,%(village_name)s,
                          %(village_lgd_code)s, %(season)s, %(irrigation_type)s, %(sown_area)s, %(area_unit)s)
                          ON CONFLICT (state_code, year, survey_status, crop_name, crop_code, village_lgd_code, season, irrigation_type, area_unit, village_name, district_name) DO UPDATE 
                          SET sown_area = EXCLUDED.sown_area;
                            ''', row.to_dict())
          # total_df.to_sql('stgdcsdata', self.engine, schema='staging', if_exists="append", index=False, chunksize=50000)
          self.conn.commit()
        fileUploadUtils().moveProcessedFile('processed', file, upload_timestamp=None)
        log.info(f"Data for tx_id {file.split('/')[-1].split('.json')[0]} inserted into db.")
      return {'success' : True, 'message':f"Data for tx_id {file.split('/')[-1].split('.json')[0]} inserted into db."}
    except Exception as e:
        log.critical('Error in '+self.processDataInPages.__name__+f': {e}')
        return {'success' : False, 'message' : 'Error in '+self.processDataInPages.__name__+f': {e}'}
  def fetchRequestParams(self, tx_id):
    try:
      log.info(f'Fetching requested state code, year, season for tx_id : {tx_id}')
      self.cur.execute(f'''SELECT seek_api_request->'message'->'search_request'->0->'search_criteria'->'query'->'query_params'->0->'state_lgd_code' AS state_lgd_code,
                          seek_api_request->'message'->'search_request'->0->'search_criteria'->'query'->'query_params'->0->'year' AS year,
                          seek_api_request->'message'->'search_request'->0->'search_criteria'->'query'->'query_params'->0->'season' AS season
                          FROM staging.dcs_seekapi_metadata
                          where transaction_id = '{tx_id}' ''')
      res = self.cur.fetchall()[0]
      state = res[0]
      year = res[1]
      season = res[2]
      return {'success' : True, 'state' : state, 'year': year,'season':season}
    except Exception as e:
      log.critical('Error in '+self.fetchRequestParams.__name__+f': {e}')
      return {'success' : False, 'message' : 'Error in '+self.fetchRequestParams.__name__+f': {e}'} 
  def callSeekAPI(self, seek_auth_token, year, state, season, page_size, page_num):
    log.info(f"Requesting data for {year, state, season, page_size, page_num}")
    payload = json.dumps({
      "signature": "Signature string",
      "header": {
        "version": "0.1.0",
        "message_id": self.msg_id,
        "message_ts": self.timestamp,
        "sender_id": "registry.example.org",
        "sender_uri": self.on_seek_api_url,
        "receiver_id": "https://agristack.gov.in",
        "total_count": 1,
        "is_msg_encrypted": False
      },
      "message": {
        "transaction_id": self.tx_id,
        "search_request": [
          {
            "reference_id": self.rf_id,
            "timestamp": self.timestamp,
            "search_criteria": {
              "query_type": "namedQuery",
              "reg_type": "agristack_farmer",
              "query": {
                  "query_name":"agristack_croparea_v0_get_data",
                "mapper_id": "i5:o5",
                "query_params": [
                  {
                    "state_lgd_code": str(state),
                    "season": season,
                    "year": year
                  }
                ]
              },
              "sort": [
                {
                  "attribute_name": "year",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "survey_status",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "crop_name",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "crop_code",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "district_name",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "village_lgd_code",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "village_name",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "season",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "irrigation_type",
                  "sort_order": "asc"
                },
                {
                  "attribute_name": "area_unit",
                  "sort_order": "asc"
                }
                
              ],
              "pagination": {
                "page_size": page_size,
                "page_number": page_num
              },
              
              "consent": {}
            },
            "locale": "en"
          }
        ]
      }
    })
    headers = {
      'Content-Type': 'application/json',
      'Authorization': f'Bearer {seek_auth_token}'
    }
    response = requests.request("POST", self.seek_api_url, headers=headers, data=payload, verify=False)
    if (page_num == 1 and page_size==1):
      tx_type = 'row_count'
    else:
      tx_type = 'data'
    self.cur.execute(f'''
    insert into staging.dcs_seekapi_metadata (transaction_id,seek_api_request,seek_api_response,seek_api_url,seek_api_status, tx_type) 
    values ('{self.tx_id}', '{payload}', '{json.dumps(response.json())}','{self.seek_api_url}', {response.status_code}, '{tx_type}')''')
    self.conn.commit()
    time.sleep(3)
    if response.status_code == 200:
      return {'success':True,'response':response, 'headers' : headers, 'payload': payload}
    else:
      return {'success':False, 'response':response, 'headers' : headers, 'payload': payload}
    
  def loadDataToReporting(self):
    try:
      log.info('''Loading data to reporting table.''')
      self.cur.execute('''
                       delete from reporting.rptdcsdata;
                        insert into reporting.rptdcsdata
                        with geomapping as (select
                            v.statekey,
                            s2.statename,
                            v.districtkey,
                            d.districtname,
                            v.subdistrictkey,
                            s.subdistrictname,
                            v.villagekey,
                            v.villagename
                        from
                            upag_p2.village v
                        join upag_p2.district d 
                        on
                            v.districtkey = d.districtkey
                            and v.statekey = d.statekey
                        join upag_p2.subdistrict s 
                        on
                            s.subdistrictkey = v.subdistrictkey
                            and s.districtkey = v.districtkey
                            and s.statekey = v.statekey
                        join upag_p2.state s2 
                        on
                            s2.statekey = v.statekey)
                        select statekey,
                        statename,
                        districtkey,
                        districtname,
                        subdistrictkey,
                        subdistrictname,
                        villagekey,
                        villagename,
                        left("year",4) as "year",
                        survey_status,
                        crop_name,
                        crop_code,
                        lower(season),
                        en_irrigation_type,
                        sown_area,
                        area_unit 
                        --dt.village_lgd_code, 
                        --dt.village_name  
                        from staging.stgdcsdata dt
                        left join geomapping gm 
                        on gm.villagekey = dt.village_lgd_code
                        join (select distinct * from reporting.dcs_irrigation_type_mapping) it
                        on trim(it.irrigation_type) = trim(dt.irrigation_type);
                       ''')
      self.conn.commit()
      log.info('''Data loaded to reporting table. ''')
      return {'success':True}
    except Exception as e:
        log.critical('Error in '+self.loadDataToReporting.__name__+f': {e}')
        return {'success' : False, 'message' : 'Error in '+self.loadDataToReporting.__name__+f': {e}'}
    
  def requestDCSData(self, year):
    for state in self.states_list:
      for season in self.season_list:
        auth_token_resp = self.getToken(self.auth_token)
        if not auth_token_resp['success']:
            return {'success' : False, 'message' : auth_token_resp['message']}
        seek_auth_token = auth_token_resp['access_token']
        log.info(f'Requesting page_count for state_code {state} for {year}, {season}')
        seek_api_resp = self.callSeekAPI(seek_auth_token,
                                          year = year, state = state, season = season, 
                                          page_size = 1, page_num = 1)
    req_data_resp = self.requestDataInPages()
    if not req_data_resp['success']:
      return {'success':False, 'message':req_data_resp['message']}
    process_data_resp = self.processDataInPages()
    if not process_data_resp['success']:
      return {'success':False, 'message':process_data_resp['message']}
    load_data_to_reporting_resp = self.loadDataToReporting()
    if load_data_to_reporting_resp['success']:
      return {'success':True}
    else:
      return {'success':False, 'message':load_data_to_reporting_resp['message']}
  
def pipeline_handler(connection,connection_type):
  log_file = f"dcs{(connection_type).lower()}etl.log"
  logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
  start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
  log.info(f"DCS ETL {connection_type} ETL job started at {start_time_stamp}")
  connect_db_resp = connection()
  if not connect_db_resp['success']:
      return {'success' : False, 'message' : connect_db_resp['message']}
  engine = connect_db_resp['engine']
  conn = connect_db_resp['conn']
  cur = connect_db_resp['cur']
  month,year = datetime.now().month,int(datetime.now().year)
  if month <=6:
      years = [year-1]
  else:
      years = [year-1,year]
  years = [(f'{year}-{int(year)+1}') for year in years]
  # year = '2023-2024'
  for year in years:
    resp = dcsETL(engine,conn,cur).requestDCSData(year = year)
  # shutil.rmtree('/home/otsietladm/dev_file_uploads/dcs-api/processed')
  end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
  if resp['success']:
        log.info(f"DCS ETL {connection_type} ETL job ended at {end_timestamp}")
        subject = f'INFO : DCS ETL {connection_type} ETL job status'
        body = f'DCS ETL {connection_type} ETL job succeeded. Please check the attached logs for details.'
  else:
      log.critical(f"DCS ETL {connection_type} ETL job ended at {end_timestamp}")
      subject = f'CRITICAL : DCS ETL {connection_type} ETL job status'
      body = f"DCS ETL {connection_type} ETL job failed. Error : {resp['message']} Please check the attached logs for details."
  total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
  log.info(f"Execution time of  DCS ETL{connection_type}ETL is {round(total_time,3)} Minutes")
  job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
  emailClient().send_mail(subject, body, log_file)
  log.handlers.clear()
  os.remove(log_file)
  conn.close()

if __name__ == '__main__':
  DB_CLIENTS = ['DEV', 'STAGING','PRODUCTION']
  for client in DB_CLIENTS:
      pipeline_handler(dbClients(client).connectToDb,client.title())
