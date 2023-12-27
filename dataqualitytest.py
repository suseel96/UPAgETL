#Imports
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

import json
import pandas as pd
import numpy as np
import math
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from etl_utils.utils import commonUtills, emailClient, dbClients
import pytz
from jinja2 import Template
from psycopg2 import sql
import warnings
warnings.filterwarnings('ignore')



# class emailClient:
#     def __init__(self):
#         self.EMAIL_SENDER = os.environ['EMAIL_SENDER']
#         self.EMAIL_RECEIVER = os.environ['EMAIL_RECEIVER']
#         self.EMAIL_PASSWORD = os.environ['EMAIL_PASSWORD']
#     def send_mail(self, subject, body, file_name=None):
#         msg = MIMEMultipart()
#         # storing the senders email address
#         msg['From'] = self.EMAIL_SENDER
#         # storing the receivers email address
#         if file_name:
#             msg['To'] = self.EMAIL_RECEIVER
#         else:
#             msg['To'] = self.EMAIL_SENDER
#         # storing the subject
#         msg['Subject'] = subject
#         # string to store the body of the mail
#         body = body
#         # attach the body with the msg instance
#         if file_name:
#             msg.attach(MIMEText(body, 'plain'))
#             # open the file to be sent
#             attachment = open(file_name, "rb")
#             # instance of MIMEBase and named as p
#             p = MIMEBase('application', 'octet-stream')
#             # To change the payload into encoded form
#             p.set_payload((attachment).read())
#             # encode into base64
#             encoders.encode_base64(p)
#             p.add_header('Content-Disposition', "attachment; filename= %s" % file_name)
#             # attach the instance 'p' to instance 'msg'
#             msg.attach(p)
#         else:
#             msg.attach(MIMEText(body, 'html'))
#         # creates SMTP session
#         s = smtplib.SMTP('smtp.gmail.com', 587)
#         # start TLS for security
#         s.starttls()
#         # Authentication
#         s.login(msg['From'], self.EMAIL_PASSWORD)
#         # Converts the Multipart msg into a string
#         text = msg.as_string()
#         # sending the mail
#         s.sendmail(msg['From'], msg['To'].split(','), text)
#         # terminating the session
#         s.quit()



class dataQualityCheck:
    def __init__(self, source_name, engine, conn, cur):
        self.source_name = source_name
        self.stg_engine = engine
        self.stg_conn = conn
        self.stg_cur = cur
        self.schema_name = 'reporting'
        self.table_name = 'data_quality_master'
        self.dq_master_df = pd.read_sql(f''' SELECT * from {self.schema_name}."{self.table_name}" where source_name = '{self.source_name}' order by seq_id''', self.stg_conn)
    @staticmethod
    def highlight_positive_values(val):
        color = 'background-color: orange' if (val > 0) or str(val) == 'no' else ''
        return color
    @staticmethod
    def highlight_no_values(val):
        color = 'background-color: orange' if val == 'no' else ''
        return color
    def generateHTMLfromDataframes(self, table_names):
        try:
            border_styles = [
                {'selector': 'table', 'props': [('border-collapse', 'collapse')]},
                {'selector': 'th, td', 'props': [('border', '1px solid black')]},
                {'selector': 'th', 'props': [('background-color', '#a6a6a6')]},
                # {'selector': 'th, td', 'props': [('border', '1px dashed black')]},
                {'selector': 'th, td', 'props': [('border', 'outset 2px black')]},
                {'selector': 'th, td', 'props': [('border', '1px dotted black')]},
            ]
            html_code = "<html>\n<head></head>\n<body>\n"
            for table in table_names:
                html_code += f"<h2>{list(table.keys())[0]}</h2>\n"
                html_code += list(table.values())[0].set_table_styles(border_styles).to_html(index=False)

            # Finish the HTML code
            html_code += "</body>\n</html>"
            # Write the HTML code to a file
            with open("output.html", "w") as file:
                file.write(html_code)
            return {'success':True, 'data': html_code}
        except Exception as e:
            return {'success':False, 'message': f'Error in '+self.generateHTMLfromDataframes.__name__+f': {e}'}
    def columnsNullCheck(self):
        try:
            count_df_list = []
            for iter_id, row in self.dq_master_df.iterrows():
                numerical_columns = self.dq_master_df['numerical_columns'][iter_id]
                categorical_columns = self.dq_master_df['categorical_columns'][iter_id]
                table_name = self.dq_master_df['table_name'][iter_id]
                null_df = pd.DataFrame()
                non_null_df = pd.DataFrame()
                unique_df = pd.DataFrame()
                for col in categorical_columns+numerical_columns:
                    col_type = 'Numerical' if col in numerical_columns else 'Categorical'
                    null_df = pd.concat([null_df, pd.read_sql(f''' SELECT '{col}' column,'{col_type}' col_type, count(*) as null_count from {table_name} where "{col}" is null ''', self.stg_conn)], ignore_index=True)
                    non_null_df = pd.concat([non_null_df, pd.read_sql(f''' SELECT '{col}' column,'{col_type}' col_type, count(*) as non_null_count from {table_name} where "{col}" is not null ''', self.stg_conn)], ignore_index=True)
                    unique_df = pd.concat([unique_df, pd.read_sql(f''' SELECT '{col}' column,'{col_type}' col_type, count(distinct "{col}") as unique_count from {table_name}''', self.stg_conn)], ignore_index=True)
                self.stg_cur.execute(f''' SELECT count(*) as row_count from {table_name} ''')
                res = self.stg_cur.fetchone()
                row_count = res[0]
                count_df = pd.merge(null_df, non_null_df, on=['column', 'col_type'])
                count_df = pd.merge(count_df, unique_df, on=['column', 'col_type'])
                count_df.insert(2,'row_count',row_count)
                count_df = count_df.style.map(self.highlight_positive_values, subset=['null_count'])
                count_df_list.append({f'Null and unique count checks for {table_name}': count_df})
            return {'success':True, 'count_df_list':count_df_list}
        except Exception as e:
            return {'success':False, 'message': f'Error in '+self.columnsNullCheck.__name__+f': {e}'}
    def uniqueColumnsCheck(self):
        try:
            unique_df_list = []
            unique_df = pd.DataFrame()
            for iter_id, row in self.dq_master_df.iterrows():
                categorical_columns = self.dq_master_df['categorical_columns'][iter_id]
                table_name = self.dq_master_df['table_name'][iter_id]
                select_clause = sql.SQL(', ').join(map(sql.Identifier, categorical_columns))
                query = sql.SQL("""
                    SELECT {select_clause}, COUNT(*) AS row_count
                    FROM {table_name}
                    GROUP BY {select_clause}
                    HAVING COUNT(*)>1
                    """).format(select_clause=select_clause, table_name=sql.SQL(table_name))
                self.stg_cur.execute(query)
                res = self.stg_cur.fetchall()
                if res:
                    unique_df = pd.concat([unique_df, pd.DataFrame(data={'table_name': table_name, 'unique_columns': ', '.join(categorical_columns), 'unique':'no'}, index=[0])], ignore_index=True)
                else:
                    unique_df = pd.concat([unique_df, pd.DataFrame(data={'table_name': table_name, 'unique_columns': ', '.join(categorical_columns), 'unique':'yes'}, index=[0])], ignore_index=True)                
            unique_df = unique_df.style.applymap(self.highlight_no_values, subset=['unique'])
            unique_df_list.append({f'Unique keys check': unique_df})
            return {'success':True, 'unique_df_list':unique_df_list}
        except Exception as e:
            return {'success':False, 'message': f'Error in '+self.uniqueColumnsCheck.__name__+f': {e}'}
    def performDataQualityChecks(self):
        try:
            final_df_list = []
            null_check_resp = self.columnsNullCheck()
            if not null_check_resp['success']:
                return {'success':False, 'message' :null_check_resp['message']}
            count_df_list = null_check_resp['count_df_list']
            unique_check_resp = self.uniqueColumnsCheck()
            if not unique_check_resp['success']:
                return {'success':False,'message': unique_check_resp['message']}
            unique_df_list = unique_check_resp['unique_df_list']
            final_df_list = final_df_list + count_df_list + unique_df_list
            generate_html_resp = self.generateHTMLfromDataframes(final_df_list)
            if not generate_html_resp['success']:
                return {'success':False,'message': generate_html_resp['message']}
            else:
                return {'success':True, 'html_content':generate_html_resp['data']}
        except Exception as e:
            return {'success':False, 'message': f'Error in '+self.performDataQualityChecks.__name__+f': {e}'}


def pipeline_handler(connection,connection_type, source):
    connect_to_db_resp = connection()
    if connect_to_db_resp['success']: 
        engine = connect_to_db_resp['engine']
        conn = connect_to_db_resp['conn']
        cur = connect_to_db_resp['cur']
    else:
        return {'success' : False, 'message' : connect_to_db_resp['message']}
    resp=dataQualityCheck(source, engine,conn,cur).performDataQualityChecks()
    if resp['success']:
        subject = f"INFO :{connection_type} Data quality checks successful"
        body = f"{connection_type} Data quality checks sucecssful."
    else:
        print(resp['message'])
        subject = f"CRITICAL : {connection_type} Data quality checks failed"
        body = f"{connection_type} Data quality checks failed. Error : {resp['message']}"
    emailClient().send_mail(subject, resp['html_content'])
    conn.close()

source = 'pmfbycce'
DB_CLIENTS = ['STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title(), source)
