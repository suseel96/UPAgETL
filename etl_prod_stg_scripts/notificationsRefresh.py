import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))


from etl_utils.utils import emailClient,dbClients,commonUtills
import pytz

from datetime import timedelta, datetime
import logging
import time

log = logging.getLogger()

# EMAIL_SENDER = 'upagscripts@gmail.com'
# EMAIL_RECEIVER = 'saibabu.j@gmail.com'
# EMAIL_PASSWORD = 'iesnszwcnzbmsrij'

def notify(cur,conn):
        try:
                cur.execute(''' call upag.sp_Notifications() ''')
                conn.commit()
                return {'success':True}
        except Exception as e:
            log.critical('Error in '+notify.__name__+f': {e}')
            return {'success' : False, 'message' : 'Error in '+notify.__name__+f': {e}'}


def pipeline_handler(connection,connection_type):
        log_file = f"{(connection_type).lower()}notifications.log"
        logging.basicConfig(filename = log_file, filemode='w', level=logging.INFO,
        format='%(asctime)s - %(name)-12s - %(levelname)-4s - %(filename)s - %(funcName)s -%(lineno)d - %(message)s')
        start_time_stamp = datetime.now(pytz.timezone('Asia/Kolkata'))
        connect_to_db_resp = connection()
        if connect_to_db_resp['success']: 
                engine = connect_to_db_resp['engine']
                conn = connect_to_db_resp['conn']
                cur = connect_to_db_resp['cur']
        else:
               return {'success' : False, 'message' : connect_to_db_resp['message']}
        notify_resp = notify(cur,conn)
        if  notify_resp['success']:
                
                log.info(f'{connection_type} call upag.sp_Notifications() Refreshed')
                subject = f"INFO : {connection_type} Notifications refreshed "
                body = f"INFO : {connection_type} Notifications refreshed on {datetime.now()}."
                end_timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
                total_time = ((end_timestamp - start_time_stamp).total_seconds())/60
                log.info(f"Execution time of  notificationrefresh{connection_type}ETL is {round(total_time,3)} Minutes")
                job_stat_resp = commonUtills().updateJobStatusInDB(os.path.abspath(__file__).split('/')[-1],datetime.today().date(),start_time_stamp,end_timestamp,notify_resp['success'],log_file,cur,conn)
                emailClient().send_mail(subject, body, log_file)
                log.handlers.clear()
                os.remove(log_file)
                conn.close()

DB_CLIENTS = ['PRODUCTION','STAGING']
for client in DB_CLIENTS:
    pipeline_handler(dbClients(client).connectToDb,client.title())