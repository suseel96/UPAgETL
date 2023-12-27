# IMPORTS
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import pandas as pd
import numpy as np
import pytz
from etl_utils.utils import emailClient,dbClients,commonUtills
from dotenv import load_dotenv
import logging
from datetime import timedelta, datetime
import time
import calendar
import zipfile
import shutil
import requests
import warnings
warnings.filterwarnings('ignore')
# LOGGING CONFIGURATION
log = logging.getLogger()

class dgcisETL:
    def __init__(self,engine,conn,cur,connection_type):
        self.engine = engine
        self.conn = conn
        self.cur =cur
        self.connection_type=connection_type
        self.DB_STAGING_SCHEMA = 'staging'
        self.DB_STAGING_TABLE_NAME_IMPORT = 'stgDGCISImport'
        self.DB_STAGING_TABLE_NAME_EXPORT = 'stgDGCISExport'
        self.path = '/home/otsietladm/Source files'
        self.files = None
        self.month_mapping = dict((month, index) for index, month in enumerate(calendar.month_name) if month)
        self.country_mapping_dict=None
        self.code_mapping_dict=None
        self.iso_mapping_dict=None

    def get_dates_to_genrate(self):
        try:
            log.info("Starting Genrating Dates")
            query="""SELECT
                (SELECT MAX("YearMonthCode") FROM staging."stgDGCISImport" where "SourceYear" in (2023,2022)) AS "Import",
                (SELECT MAX("YearMonthCode") FROM staging."stgDGCISExport" where "SourceYear" in (2023,2022)) AS "Export"
                """
            dates_df=pd.read_sql_query(query,self.conn)
            max_dates=dates_df.to_dict('records')[0]
            dates={}
            for type_ in max_dates:
                start=f'{max_dates[type_][:4]}-{max_dates[type_][4:]}-01'
                end=datetime.today()
                date_range=pd.date_range(start,end,freq='M').strftime('%b-%Y').tolist()
                if date_range[1:]:
                    dates[type_]=date_range[1:]
            if dates:
                log.info("Dates genrated successfully.")
                return {'success':True,'data':dates}
            else :
                log.info("No dates to genrate.")
                return {"success":False,"message":"No dates to genrate"}
        except Exception as e:
                log.critical('Error in '+self.get_dates_to_genrate.__name__+f': {e}')
                return {'success' : False, 'message' : 'Error in '+self.get_dates_to_genrate.__name__+f': {e}'}

    def get_file(self,month_year,type_):
        try:
            log.info(f"Fetching Data for {month_year} {type_}")
            url="https://119.226.26.106/dgcis/dataqueryDownload"  #alternative url if below one does not work
            url="https://ftddp.dgciskol.gov.in/dgcis/dataqueryDownload"  
            payload = {
                'eximp': type_[0],
                'datepicker': month_year ,
                'datepicker1': month_year,
                'commodities': 'A',
                'countries': 'A',
                'countries_org': 'undefined',
                'regions': 'undefined',
                'ports': 'undefined',
                'sorted': 'Order By HS_CODE,CTY',
                'currency': 'B',
                'digites': '8',
                'reg': '2',
                'type': '4',
                'description': '1'
            }
            response = requests.get(url,params=payload,verify=False)
            if response.status_code == 200:
                file_content = response.content
                if file_content==b'':
                    log.info(f"No data for {month_year}-{type_}")
                    return {"success":False,"message":f"No data for {month_year}-{type_}"}
                os.makedirs("/tmp/dgcis_files_zip", exist_ok=True)
                file_path = f'/tmp/dgcis_files_zip/dgcis_{month_year}_{type_}.zip'
                with open(file_path, 'wb') as file:
                    file.write(file_content)
                log.info(f"Data fetched successfully for {month_year} {type_}")
                return {"success":True}

        except Exception as e:
                log.critical('Error in '+self.get_file.__name__+f': {e}')
                return {'success' : False, 'message' : 'Error in '+self.get_file.__name__+f': {e}'}
        
    def unzip_file(self,month_year,type_):
        try:
            log.info(f"Unziping file for {month_year} {type_}")
            file_path='/tmp/dgcis_files_zip/'
            files=os.listdir(file_path)
            extracted_dir = "/home/otsietladm/Source files/"
            for file in files:
                with zipfile.ZipFile(file_path+file, 'r') as zip_ref:
                    for member in zip_ref.infolist():
                        new_name = f"dgcis_{month_year}_{type_}_" + member.filename.split("_")[-1]  
                        zip_ref.extract(member, path=file_path)
                        os.rename(os.path.join(file_path, member.filename), os.path.join(extracted_dir, new_name))
            shutil.rmtree(file_path)
            log.info(f"Unziping successfull for {month_year} {type_}")
            return {"success":True}

        except Exception as e:
            log.critical('Error in '+self.unzip_file.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.unzip_file.__name__+f': {e}'}
        
    def InitializeMapings(self):
        try:
            log.info("Initializing Mapings.")
            
            mapping_df = pd.read_sql(''' SELECT x.* FROM reporting."CountryMapping" x
            ORDER BY x."Country"''', self.engine)
            mapping_df['CountryAliasName'] = mapping_df['CountryAliasName'].apply(lambda x: ','.join(x))
            self.country_mapping_dict = mapping_df[['CountryAliasName','Country']].set_index('CountryAliasName').to_dict()['Country']
            self.code_mapping_dict = mapping_df[['Country', 'CountryCode']].set_index('Country').to_dict()['CountryCode']
            self.iso_mapping_dict = mapping_df[['Country', 'iso_a3_eh']].set_index('Country').to_dict()['iso_a3_eh']
            log.info("Mapings initialized.")
            return {"success":True}
        except Exception as e:
            log.critical('Error in '+self.InitializeMapings.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.InitializeMapings.__name__+f': {e}'}
        
    def CountryLookup(self,country_name):
        try:
            for key, val in self.country_mapping_dict.items():
                if country_name.lower() in key.lower().split(','):
                    return val
        except Exception as e:
            log.critical('Error in '+self.CountryLookup.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.CountryLookup.__name__+f': {e}'}
            
    def Standardization(self,df):
        try:
            log.info("Standardizing country names.")
            if 'Country of CONSIGNMENT' in df.columns:
                df['StandardizedCountry'] = df['Country of CONSIGNMENT'].apply(self.CountryLookup)
            elif 'Country of DESTINATION' in df.columns:
                df['StandardizedCountry'] = df['Country of DESTINATION'].apply(self.CountryLookup)
            df['CountryCode'] = df['StandardizedCountry'].apply(lambda x: self.code_mapping_dict[x])
            df['CountryISO'] = df['StandardizedCountry'].apply(lambda x: self.iso_mapping_dict[x])
            if df['StandardizedCountry'].isna().sum() > 0:
                log.warning("Found NA values in country name after standardization")
                return {"success":False,"message":"Found NA values in country name after standardization."}
            log.info("Country names standardized.")
            return {"success":True,"data":df}
        except Exception as e:
            log.critical('Error in '+self.Standardization.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.Standardization.__name__+f': {e}'}
    
    def TransformData(self,df,file):
        try:
            log.info(f"Transforming Data {file[16:]}")
            df['Chapter'] = df['Hs Code'].str[:2]
            df['Heading'] = df['Hs Code'].str[:4]
            df['Sub-Heading'] = df['Hs Code'].str[:6]
            df['YearMonth'] = df.apply(lambda x:x['SourceMonth'][:3]+' - '+str(x['SourceYear']), axis=1)
            df['MonthCode'] = df['SourceMonth'].apply(lambda x:self.month_mapping[x])
            df['YearMonthCode'] = df.apply(lambda x: str(x['SourceYear'])+str(x['MonthCode']) 
                                    if len(str(x['MonthCode']))>1 else str(x['SourceYear'])+'0'+str(x['MonthCode']), axis=1)
            df['FinancialYear'] = df.apply(lambda x: x['SourceYear']-1 if x['SourceMonth'] in ['January', 'February', 'March'] else
                                        x['SourceYear'], axis=1)
            if 'Country of CONSIGNMENT' in df.columns:
                df = df.melt(id_vars=['Commodity', 'Country of CONSIGNMENT', 'Unit', 'Quantity','Value_in_INR', 'Value_in_USD', 'SourceMonth', 
                                      'SourceYear','StandardizedCountry', 'CountryCode', 'CountryISO', 'YearMonth', 'MonthCode', 'YearMonthCode',
                                    'FinancialYear'], value_name='HSCode', var_name='CodeType')
                
                df = df[['Commodity', 'CodeType', 'HSCode' ,'Country of CONSIGNMENT', 'StandardizedCountry', 'CountryCode', 'CountryISO',
                            'Unit', 'Quantity', 'Value_in_INR', 'Value_in_USD', 'SourceYear', 'SourceMonth','YearMonth',
                            'MonthCode', 'YearMonthCode', 'FinancialYear']]
                log.info(f"{file[17:]} transformed.")
                return {"success":True,"type":"import","data":df}
            
            elif 'Country of DESTINATION' in df.columns:
                df = df.melt(id_vars=['Commodity', 'Country of DESTINATION', 'Unit', 'Quantity','Value_in_INR', 'Value_in_USD', 'SourceMonth',
                                      'SourceYear','StandardizedCountry', 'CountryCode', 'CountryISO', 'YearMonth', 'MonthCode', 'YearMonthCode',
                                      'FinancialYear'], value_name='HSCode', var_name='CodeType')
                df = df[['Commodity', 'CodeType', 'HSCode' ,'Country of DESTINATION', 'StandardizedCountry', 'CountryCode', 'CountryISO',
                                    'Unit', 'Quantity', 'Value_in_INR', 'Value_in_USD', 'SourceYear', 'SourceMonth','YearMonth',
                                    'MonthCode', 'YearMonthCode', 'FinancialYear']]
                log.info(f"{file[16:]} transformed.")
                return {"success":True,"type":"export","data":df}
            else :
                log.warning("Error while tranforming data no country of destination/consignment in dataframe")
                return {"success":False,"message":"Error while tranforming data no country of destination/consignment in dataframe"}
        except Exception as e:
            log.critical('Error in '+self.TransformData.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.TransformData.__name__+f': {e}'}
        
    def ReadDataFromFile(self, file):
        try:
            log.info(f'Reading data  from {file[16:]}')
            if file.split('.')[-1] == 'xls':
                df = pd.read_excel(file, header=1, usecols='A:G', dtype={'Hs Code': object})
            df['SourceMonth'] = df.columns[4].split()[0].split(',')[0]
            df['SourceYear'] = int(df.columns[4].split()[1])+2000
            df.columns = ['Quantity' if 'Qty'in col else 'Value_in_INR' if 'INR' in col else
                        'Value_in_USD' if 'US $' in col else col for col in df.columns]
            log.info(f'Data read  from {file[16:]} successfully.')
            return {'success' : True, 'data' : df}
        
        except Exception as e:
            log.critical('Error in '+self.ReadDataFromFile.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.ReadDataFromFile.__name__+f': {e}'}
    
    def LoadDataToStaging(self,df,table_name):
        try:
            log.info(f'Loading data into {self.DB_STAGING_SCHEMA}.{table_name}')
            df.to_sql(f'{table_name}', self.engine, schema=f'{self.DB_STAGING_SCHEMA}', if_exists="append", index=False, chunksize=100000)
            self.conn.commit()
            log.info(f'Data loaded into {self.DB_STAGING_SCHEMA}.{table_name}')
            return {'success' : True}
        except Exception as e:
            log.critical('Error in '+self.loadDataToStaging.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.loadDataToStaging.__name__+f': {e}'}
    
    def LoadToReporting(self):
        try:
            log.info("Loading data into reporting")
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" where "DataSource" ='DGCIS' and "SeqNo" =1""")
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(querytext)
            self.conn.commit() 
            
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" where "DataSource" ='DGCIS' and "SeqNo" =2""")
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(querytext)
            self.conn.commit() 
            
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" where "DataSource" ='DGCIS' and "SeqNo" =3""")
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(querytext)
            self.conn.commit() 
            
            self.cur.execute( """
                select "QueryText"  from reporting."ETLFlowMaster" where "DataSource" ='DGCIS' and "SeqNo" =4""")
            querytext= self.cur.fetchall()[0][0]
            self.cur.execute(querytext)
            self.conn.commit() 
            log.info("Data loaded into reporting")
            return {'success' : True}
        
        except Exception as e:
            log.critical('Error in '+self.LoadToReporting.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+self.LoadToReporting.__name__+f': {e}'}

    def startdgcisETL(self):
        if self.connection_type=='PRODUCTION':
            dates_resp=self.get_dates_to_genrate()
            if not dates_resp["success"]:
                return {"success":False,"message":dates_resp["message"]}
            dates=dates_resp["data"]
            for type_ in  dates:
                for month_year in dates[type_]:
                    get_file_resp=self.get_file(month_year=month_year,type_=type_)
                    if not get_file_resp["success"] :
                        break
                    unzip_resp=self.unzip_file(month_year=month_year,type_=type_)
                    if not unzip_resp["success"]:
                        return {"success":False,"message":unzip_resp["message"]} 
                    
        files = os.listdir(self.path)
        self.files=[self.path +'/'+file for file in files]
        if not files:
            return {"success":False,"message":"No file to process"}
        
        init_maping_resp=self.InitializeMapings()
        if not init_maping_resp["success"]:
            return {"success":False,"message":init_maping_resp["message"]}

        for file in self.files:
            read_data_resp=self.ReadDataFromFile(file)

            if not read_data_resp:
                return {"success":False,"message":read_data_resp["message"]}
            std_resp = self.Standardization(read_data_resp["data"])

            if not std_resp["success"]:
                return {"success":False,"message":std_resp["message"]}
            trans_resp=self.TransformData(std_resp["data"],file)

            if not trans_resp["success"]:
                return {"success":False,"message":trans_resp["message"]}
            tranformed_df=trans_resp["data"]

            if trans_resp["type"]=="import":
                load_to_staging_resp=self.LoadDataToStaging(df=tranformed_df,table_name=self.DB_STAGING_TABLE_NAME_IMPORT)
            else :
                load_to_staging_resp=self.LoadDataToStaging(df=tranformed_df,table_name=self.DB_STAGING_TABLE_NAME_EXPORT)
            if not load_to_staging_resp["success"]:
                return {"success":False,"message":load_to_staging_resp["message"]}
            if self.connection_type!="Production":
                os.rename(file, file.replace('Source files','Processed files'))

        load_to_reporting_resp=self.LoadToReporting()
        if not load_to_reporting_resp["success"]:
            return {"success":False,"message":load_to_reporting_resp["message"]}
        
        return {"success":True}
    
def pipeline_handler(connection,connection_type):
    log_file = f"dgcis{connection_type}.log"
    logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    log.info(f"DGCIS {connection_type} ETL job started at {start_time_stamp}")
    resp = dgcisETL(engine,conn,cur,connection_type).startdgcisETL()
    end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    if resp['success']:
        log.info(f"{connection_type} DGCIS ETL job ended at {end_timestamp}")
        subject = f"INFO : {connection_type} DGCIS ETL job status"
        body = f"{connection_type} DGCIS ETL job succeeded. Please check the attached logs for details."
    else:
        log.info(resp["message"])
        log.critical(f"{connection_type} DGCIS ETL job ended at {end_timestamp}")
        subject = f'CRITICAL : {str(connection).split(" ")[2][18:-2]} DGCIS ETL job status'
        body = f"{connection_type} DGCIS ETL job failed. Error : {resp['message']} Please check the attached logs for details."
        
    total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
    log.info(f"Execution time of DGCIS{(connection_type).lower()}etl is {round(total_time,3)} Minutes")
    job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,resp['success'],log_file,cur,conn)
    emailClient().send_mail(subject, body, log_file)
    log.handlers.clear()
    os.remove(log_file)
    conn.close()
DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())