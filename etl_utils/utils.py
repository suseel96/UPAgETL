import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import sys
import os
import logging
import smtplib
from jinja2 import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import re

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

load_dotenv()

class dbClient:
    def __init__(self):
        pass
    def connectToStagingDb(self):
        try:
            engine = create_engine(f"{os.environ['STAGING_DATABASE_TYPE']}://{os.environ['STAGING_DATABASE_USERNAME']}:{os.environ['STAGING_DATABASE_PASSWORD']}@{os.environ['STAGING_DATABASE_HOST']}:{os.environ['STAGING_DATABASE_PORT']}/{os.environ['STAGING_DATABASE_DBNAME']}")
            conn = engine.raw_connection()
            cur = conn.cursor()
            return {'success' : True, 'engine' : engine, 'conn' : conn, 'cur' : cur}
        except Exception as e:
            return {'success' : False, 'message' : 'Error in '+self.connectToStagingDb.__name__+f': {e}'}
    def connectToProductionDb(self):
        try:
            engine = create_engine(f"{os.environ['PRODUCTION_DATABASE_TYPE']}://{os.environ['PRODUCTION_DATABASE_USERNAME']}:{os.environ['PRODUCTION_DATABASE_PASSWORD']}@{os.environ['PRODUCTION_DATABASE_HOST']}:{os.environ['PRODUCTION_DATABASE_PORT']}/{os.environ['PRODUCTION_DATABASE_DBNAME']}")
            conn = engine.raw_connection()
            cur = conn.cursor()
            return {'success' : True, 'engine' : engine, 'conn' : conn, 'cur' : cur}
        except Exception as e:
            return {'success' : False, 'message' : 'Error in '+self.connectToProductionDb.__name__+f': {e}'}
        
class dbClients:
    def __init__(self, env):
        self.ENV = env
        self.DATABASE_TYPE = os.environ[f'{self.ENV}_DATABASE_TYPE']
        self.DATABASE_USERNAME = os.environ[f'{self.ENV}_DATABASE_USERNAME']
        self.DATABASE_PASSWORD = os.environ[f'{self.ENV}_DATABASE_PASSWORD']
        self.DATABASE_HOST = os.environ[f'{self.ENV}_DATABASE_HOST']
        self.DATABASE_PORT = os.environ[f'{self.ENV}_DATABASE_PORT']
        self.DATABASE_DBNAME = os.environ[f'{self.ENV}_DATABASE_DBNAME']
    def connectToDb(self):
        try:
            engine = create_engine(f"{self.DATABASE_TYPE}://{self.DATABASE_USERNAME}:{self.DATABASE_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DBNAME}")
            conn = engine.raw_connection()
            cur = conn.cursor()
            return {'success' : True, 'engine' : engine, 'conn' : conn, 'cur' : cur}
        except Exception as e:
            return {'success' : False, 'message' : 'Error in '+self.connectToDb.__name__+f': {e}'}

class emailClient:
    def __init__(self):
        self.EMAIL_SENDER = os.environ['EMAIL_SENDER']
        self.EMAIL_RECEIVER = os.environ['EMAIL_RECEIVER']
        self.EMAIL_PASSWORD = os.environ['EMAIL_PASSWORD']
    def send_mail(self, subject, body, file_name=None):
        msg = MIMEMultipart()
        # storing the senders email address
        msg['From'] = self.EMAIL_SENDER
        # storing the receivers email address
        if file_name:
            msg['To'] = self.EMAIL_RECEIVER
        else:
            msg['To'] = 'suseel.nookavarapu@otsi.co.in'
        # storing the subject
        msg['Subject'] = subject
        # string to store the body of the mail
        body = body
        # attach the body with the msg instance
        if file_name:
            msg.attach(MIMEText(body, 'plain'))
            # open the file to be sent
            attachment = open(file_name, "rb")
            # instance of MIMEBase and named as p
            p = MIMEBase('application', 'octet-stream')
            # To change the payload into encoded form
            p.set_payload((attachment).read())
            # encode into base64
            encoders.encode_base64(p)
            p.add_header('Content-Disposition', "attachment; filename= %s" % file_name)
            # attach the instance 'p' to instance 'msg'
            msg.attach(p)
        else:
            msg.attach(MIMEText(body, 'html'))
        # creates SMTP session
        s = smtplib.SMTP('smtp.gmail.com', 587)
        # start TLS for security
        s.starttls()
        # Authentication
        s.login(msg['From'], self.EMAIL_PASSWORD)
        # Converts the Multipart msg into a string
        text = msg.as_string()
        # sending the mail
        s.sendmail(msg['From'], msg['To'].split(','), text)
        # terminating the session
        s.quit()
    # def send_mail(self, subject, body, file_name):
    #     msg = MIMEMultipart()
    #     # storing the senders email address
    #     msg['From'] = self.EMAIL_SENDER
    #     # storing the receivers email address
    #     msg['To'] = self.EMAIL_RECEIVER
    #     # storing the subject
    #     msg['Subject'] = subject
    #     # string to store the body of the mail
    #     body = body
    #     # attach the body with the msg instance
    #     msg.attach(MIMEText(body, 'plain'))
    #     # open the file to be sent
    #     attachment = open(file_name, "rb")
    #     # instance of MIMEBase and named as p
    #     p = MIMEBase('application', 'octet-stream')
    #     # To change the payload into encoded form
    #     p.set_payload((attachment).read())
    #     # encode into base64
    #     encoders.encode_base64(p)
    #     p.add_header('Content-Disposition', "attachment; filename= %s" % file_name)
    #     # attach the instance 'p' to instance 'msg'
    #     msg.attach(p)
    #     # creates SMTP session
    #     s = smtplib.SMTP('smtp.gmail.com', 587)
    #     # start TLS for security
    #     s.starttls()
    #     # Authentication
    #     s.login(self.EMAIL_SENDER, self.EMAIL_PASSWORD)
    #     # Converts the Multipart msg into a string
    #     text = msg.as_string()
    #     # sending the mail
    #     s.sendmail(self.EMAIL_SENDER, self.EMAIL_RECEIVER.split(','), text)
    #     # terminating the session
    #     s.quit()
        
    
class fileUploadUtils:
    def __init__(self):
        self.folder_script_dict = {}
    def fetchFolderScriptMappings(self):
        #dev-file uploads
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/des-wholesaleprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desWholesalePricesETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/mncfc/pending"]="/home/otsietladm/etl_prod_stg_scripts/mncfcETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/des-retailprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desRetailPricesETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/ieg/pending"]="/home/otsietladm/etl_prod_stg_scripts/iegETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/des-msp/pending"]="/home/otsietladm/etl_prod_stg_scripts/desMspETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/dgcis-provisional/pending"]="/home/otsietladm/etl_prod_stg_scripts/dgcisProvisionalETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/nsso/pending"]="/home/otsietladm/etl_prod_stg_scripts/nssoETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/sammunati/pending"]="/home/otsietladm/etl_prod_stg_scripts/sammunatiETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/farmers-survey/pending"]="/home/otsietladm/etl_prod_stg_scripts/farmerSurveyETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/cost-of-cultivation/pending"]="/home/otsietladm/etl_prod_stg_scripts/costOfCultivationETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/dcs-api/pending"]="/home/otsietladm/etl_prod_stg_scripts/dcsTriggeredETL.py"
        # self.folder_script_dict["/home/otsietladm/dev_file_uploads/lus/pending"]="/home/otsietladm/etl_prod_stg_scripts/lusETL.py"

        # #stg-file uploads
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/des-wholesaleprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desWholesalePricesETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/mncfc/pending"]="/home/otsietladm/etl_prod_stg_scripts/mncfcETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/des-retailprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desRetailPricesETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/ieg/pending"]="/home/otsietladm/etl_prod_stg_scripts/iegETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/des-msp/pending"]="/home/otsietladm/etl_prod_stg_scripts/desMspETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/dgcis-provisional/pending"]="/home/otsietladm/etl_prod_stg_scripts/dgcisProvisionalETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/nsso/pending"]="/home/otsietladm/etl_prod_stg_scripts/nssoETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/sammunati/pending"]="/home/otsietladm/etl_prod_stg_scripts/sammunatiETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/farmers-survey/pending"]="/home/otsietladm/etl_prod_stg_scripts/farmerSurveyETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/cost-of-cultivation/pending"]="/home/otsietladm/etl_prod_stg_scripts/costOfCultivationETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/trs-earas/pending"]="/home/otsietladm/etl_prod_stg_scripts/trsETL.py"
        # self.folder_script_dict["/home/otsietladm/stg_file_uploads/lus/pending"]="/home/otsietladm/etl_prod_stg_scripts/lusETL.py"
        # 
        #prd-file uploads
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/des-wholesaleprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desWholesalePricesETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/mncfc/pending"]="/home/otsietladm/etl_prod_stg_scripts/mncfcETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/des-retailprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desRetailPricesETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/ieg/pending"]="/home/otsietladm/etl_prod_stg_scripts/iegETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/des-msp/pending"]="/home/otsietladm/etl_prod_stg_scripts/desMspETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/dgcis-provisional/pending"]="/home/otsietladm/etl_prod_stg_scripts/dgcisProvisionalETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/nsso/pending"]="/home/otsietladm/etl_prod_stg_scripts/nssoETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/sammunati/pending"]="/home/otsietladm/etl_prod_stg_scripts/sammunatiETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/farmers-survey/pending"]="/home/otsietladm/etl_prod_stg_scripts/farmerSurveyETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/cost-of-cultivation/pending"]="/home/otsietladm/etl_prod_stg_scripts/costOfCultivationETL.py"
        self.folder_script_dict["/home/otsietladm/prd_file_uploads/lus/pending"]="/home/otsietladm/etl_prod_stg_scripts/lusETL.py"
        return self.folder_script_dict
    
    def moveProcessedFile(self, status, file_with_path, upload_timestamp=None):
        try:
            if status:
                move_to = 'processed'
            else:
                move_to = 'failed'
            file_name = file_with_path.split('/pending/')[-1]
            file_path = file_with_path.replace(file_name,'')
            if upload_timestamp is not None:
                moved_file_name = file_path.replace('pending', move_to) + f"{upload_timestamp[:-7].replace(' ','_').replace('-','')}_" + file_name 
            else:
                moved_file_name = file_path.replace('pending', move_to) + file_name 
            os.rename(file_with_path, moved_file_name)
            return {'success': True}
        except Exception as e:
            return {'success' : False, 'message' : 'Error in '+self.moveProcessedFile.__name__+f': {e}'}
        
        
    def updateFileStatusInDB(self, status, log_file, file_name, cur, conn):
        if status:
            status = 'processed'
        else:
            status = 'failed'
        try:
            with open(log_file,'r') as log_file:
                log_data = log_file.read()
        
            cur.execute(f'''update upag.agencyfileuploadstatus 
                        set etlstatus = '{status}',etlremarks = '{log_data}' 
                        where etlserverfiles = '{file_name.split("/")[-1]}' ''',conn)
            conn.commit()
            
            return {'success':True}
        except Exception as e:
            return {'success' : False, 'message' : 'Error in '+self.updateFileStatusInDB.__name__+f': {e}'}

        
class commonUtills:
    def formatColumnNames(self,df):
        try :
            df.columns = df.columns.str.lower().str.replace('"','')
            df.columns = [''.join(e if e.isalpha() else '_' for e in col) for col in df.columns]
            df.columns = [re.sub(r'_+', '_', col) for col in df.columns]
            df.columns = [col[:-1] if col[-1] == '_' else col[1:] if col[0] == '_' else col for col in df.columns]
            return {"success":True,"data":df} 
        except Exception as e:
            return {'success' : False, 'message' : 'Error in '+self.formatColumnNames.__name__+f': {e}'}
        
    def handleException(log):
        def decorator(func):
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    log.critical(f'Error in {func.__name__}: {e}')
                    return {"success": False, 'message': f'Error in {func.__name__}: {e}'}
            return wrapper
        return decorator
    
    def updateJobStatusInDB(self,etl_job , run_date , start_timestamp , end_timestamp,status,log,cur,conn):
        if status:
            status = 'Success'
        else:
            status = 'Fail'
        try:
            with open(log,'r') as log:
                log_data = log.read()
            cur.execute(f'''insert into reporting.etl_logs (etl_job , run_date , start_timestamp , end_timestamp,status,log)
            values ('{etl_job}' , '{run_date}' , '{start_timestamp}' , '{end_timestamp}','{status}','{log_data}')''')
            conn.commit()
            return {'success':True}
        
        except Exception as e:
            return {'success' : False, 'message' : 'Error in '+self.updateJobStatusInDB.__name__+f': {e}'}
        
    def generateHTMLReport(self, metrics_data, report_title):
        html_template = '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Report for {{ report_title }}</title>
            <style>
                table {
                    border-collapse: collapse;
                    width: 50%; /* Set the desired width for the table */
                }
                th, td {
                    border: 1px solid #dddddd;
                    text-align: left;
                    padding: 8px;
                }
                th {
                    background-color: #f2f2f2;
                }
                tr:nth-child(even) {
                    background-color: #f9f9f9;
                }
                .collapsible {
                    cursor: pointer;
                    padding: 18px;
                    width: 100%;
                    border: none;
                    text-align: left;
                    outline: none;
                    font-size: 15px;
                }
                .content {
                    display: none;
                    padding: 0 18px;
                    overflow: hidden;
                    border: 1px solid #f1f1f1;
                }
            </style>
        </head>
        <body>
            <h1>Data Quality Report for {{ report_title }}</h1>
            <table id="rowtable">
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                {% for metric_name, metric_value in metrics_data.items() %}
                    <tr class="collapsible">
                        <td>{{ metric_name }}</td>
                        <td>{{ metric_value }}</td>
                    </tr>
                {% endfor %}
            </table>
            
            <script>
                var coll = document.getElementsByClassName("collapsible");
                var i;

                for (i = 0; i < coll.length; i++) {
                    coll[i].addEventListener("click", function() {
                        this.classList.toggle("active");
                        var content = this.nextElementSibling;
                        if (content.style.display === "block") {
                            content.style.display = "none";
                        } else {
                            content.style.display = "block";
                        }
                    });
                }
            </script>
        </body>
        </html>
        '''
        template = Template(html_template)
        html_content = template.render(metrics_data=metrics_data, report_title=report_title)
        return html_content
            
class customLogging:
    pass