""" importing modules """
import json
import logging

import json
import datetime
import sqlalchemy
import pandas as pd

import test_download

#download code
Project_id= 'P_555'
Task_id = '1234'
test_download.download_code(Project_id,Task_id)

from utility import initiate_logging
import definitions_qc as dq
## logging module
initiate_logging('engine_log', r"C:/Users/ParulShrikhande/Desktop/Latest_code/logging/")
logging.info('logging initiated')
# reading the json file
try:
    with open(r"C:/Users/ParulShrikhande/Desktop/Latest_code/1234.json",'r', encoding='utf-8') as jsonfile:
        logging.info("reading json data started")
        json_data = json.load(jsonfile)
        logging.info("reading json data completed")
except Exception as error:
    logging.exception("error in reading json %s.", str(error))
    raise Exception("error in reading json: " + str(error)) from error

# Precheck code
pre_check = dq.qc_pre_check(json_data)


#ingestion code
if json_data["task"]["source"]["source_type"] == "csv_read":
    from csv_read_file import read
elif json_data["task"]["source"]["source_type"] == "postgres_read":
    from postgres_read_file import read
elif json_data["task"]["source"]["source_type"] == "mysql_read":
    from mysql_read_file import read
if json_data["task"]["target"]["target_type"] == "postgres_write":
    from postgres_write_file import write
elif json_data["task"]["target"]["target_type"] == "mysql_write":
    from mysql_write_file import write


# main script execution starts here
if json_data["task"]["task_type"]=="ingestion":
    logging.info("entered  in to ingestion")
    
    df=read(json_data)
    # logging.info(df.__next__())
    COUNTER=0
    for i in df :
        # logging.info(i)
        COUNTER+=1
        write(json_data, i, COUNTER)
else:
    logging.info("only ingestion available currently")


#post check code
post_check = dq.qc_post_check(json_data)

#qc report generation
qc_report = dq.qc_report(pre_check, post_check)
logging.info(qc_report)

