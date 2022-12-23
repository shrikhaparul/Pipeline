""" importing modules """
import json
import logging
import sqlalchemy
import pandas as pd
import os
import subprocess
import logging
from datetime import datetime

def download_files(Project_id,Task_id,path):
    with open(
        r""+path+'app/Program/Project/Pipeline/Task/'+Task_id+"/"+Task_id+".json","r", encoding='utf-8') as jsonfile:
        config_json = json.load(jsonfile)
    source_conn_file = config_json['task']['source']['connection_name']
    target_conn_file = config_json['task']['target']['connection_name']
    source_type = config_json['task']['source']['source_type']
    target_type = config_json['task']['target']['target_type']
    
    
    #curl command for downloading the files
    src_json = 'curl -o '+path+'app/Common/Config'+'.json '\
    'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/'+Project_id+'/'+source_conn_file+'.json'
    trgt_json = 'curl -o '+path+'app/Common/Config'+'.json '\
    'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/'+Project_id+'/'+target_conn_file+'.json'
    
    
    subprocess.call(src_json)
    subprocess.call(trgt_json)
    
  
    mapping_file='curl -o '+path+'app/Common/Scripts/engine_main'+'mapping.json '\
    'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/Files/mapping.json'

    subprocess.call(mapping_file)

    with open(r""+path+'app/Common/Scripts/engine_main'+'mapping.json',"r", encoding='utf-8') as mapjson:
        config_new_json = json.load(mapjson)
    
    
    source_file_name=(config_new_json["mapping"][source_type])
    target_file_name=(config_new_json["mapping"][target_type])
    QC_check_file=config_new_json["default"]["QC_checks"]
    Utility_file=config_new_json["default"]["Utility"]
    print(QC_check_file,Utility_file)
    
    #curl command for downloading the files
    src_py = 'curl -o '+path+'app/Common/Scripts/ingestion'+' '\
    'https://raw.githubusercontent.com/shrikhaparul/pipeline/main/Project/Files/'+source_file_name
    trgt_py = 'curl -o '+path+'app/Common/Scripts/ingestion'+' '\
    'https://raw.githubusercontent.com/shrikhaparul/pipeline/main/Project/Files/'+target_file_name
    QC_py= 'curl -o '+path+'app/Common/Scripts/dq_scripts'+' '\
    'https://raw.githubusercontent.com/shrikhaparul/pipeline/main/Project/Files/'+QC_check_file
    Utility_py= 'curl -o '+path+'app/Common/Scripts/ingestion'+' '\
    'https://raw.githubusercontent.com/shrikhaparul/pipeline/main/Project/Files/'+Utility_file
    
    #calling the subprocess to run curl command function
    subprocess.call(src_py)
    subprocess.call(trgt_py)
    subprocess.call(QC_py)
    subprocess.call(Utility_py)



def engine_main(Project_id,Task_id,path):
    import definitions_qc as dq
    try:
        with open(r""+path+Task_id+".json","r", encoding='utf-8') as jsonfile:
            logging.info("reading json data started")
            json_data = json.load(jsonfile)
            logging.info("reading json data completed")
    except Exception as error:
        logging.exception("error in reading json %s.", str(error))
        raise Exception("error in reading json: " + str(error)) from error

    try:
        with open(r""+path+"checks_mapping.json","r", encoding='utf-8') as json_data_new:
            logging.info("reading json data started")
            json_checks = json.load(json_data_new)
            logging.info("reading json data completed")
    except Exception as error:
        logging.exception("error in reading json %s.", str(error))
        raise Exception("error in reading json: " + str(error)) from error

    
    # Precheck script execution starts here
    if json_data["task"]["source"]["source_type"] == 'csv_read' or json_data["task"]["source"]["source_type"] == 'postgres_read' or\
     json_data["task"]["source"]["source_type"] == 'mysql_read' or json_data["task"]["source"]["source_type"] == 'mssql_read':
       pre_check = dq.qc_pre_check(json_data, json_checks)

    
    #file conversion code and ingestion code
    if json_data["task"]["source"]["source_type"] == "csv_read":
        from csv_read import read
    elif json_data["task"]["source"]["source_type"] == "postgres_read":
        from postgres_read import read
    elif json_data["task"]["source"]["source_type"] == "mysql_read":
        from mysql_read import read
    elif json_data["task"]["source"]["source_type"] == "csvfile_read":
        from csvfile_read import read
    elif json_data["task"]["source"]["source_type"] == "parquetfile_read":
        from parquet_read import read
    elif json_data["task"]["source"]["source_type"] == "excelfile_read":
        from excel_read import read
    elif json_data["task"]["source"]["source_type"] == "jsonfile_read":
        from json_read import read
    elif json_data["task"]["source"]["source_type"] == "xmlfile_read":
        from xml_read import read
    elif json_data["task"]["source"]["source_type"] == "textfile_read":
        from text_read import read
    elif json_data["task"]["source"]["source_type"] == "mssql_read":
        from mssql_read import read


    if json_data["task"]["target"]["target_type"] == "postgres_write":
        from postgres_write import write
    elif json_data["task"]["target"]["target_type"] == "mysql_write":
        from mysql_write import write
    elif json_data["task"]["target"]["target_type"] == "parquetfile_write":
        from parquet_write import write
    elif json_data["task"]["target"]["target_type"] == "csv_write":
        from csv_write import write
    elif json_data["task"]["target"]["target_type"] == "csvfile_write":
        from csvfile_write import write
    elif json_data["task"]["target"]["target_type"] == "excelfile_write":
        from excel_write import write
    elif json_data["task"]["target"]["target_type"] == "jsonfile_write":
        from json_write import write
    elif json_data["task"]["target"]["target_type"] == "xmlfile_write":
        from xml_write import write
    elif json_data["task"]["target"]["target_type"] == "textfile_write":
        from text_write import write
    elif json_data["task"]["target"]["target_type"] == "mssql_write":
        from mssql_write import write


    # main script execution starts here
    if json_data["task"]["source"]["source_type"] == "csv_read" or \
        json_data["task"]["source"]["source_type"] == "postgres_read" or \
        json_data["task"]["source"]["source_type"] == "mysql_read" or  json_data["task"]["target"]["target_type"] == "mssql_write" or\
        json_data["task"]["target"]["target_type"] == "mysql_write"  or\
        json_data["task"]["target"]["target_type"] == "postgres_write"      :
        df=read(json_data)
        # logging.info(df.__next__())
        COUNTER=0
        for i in df :
            # logging.info(i)
            COUNTER+=1
            write(json_data, i,COUNTER)
    elif json_data["task"]["source"]["source_type"] == "csvfile_read" or \
     json_data["task"]["source"]["source_type"] == "jsonfile_read" or \
     json_data["task"]["source"]["source_type"] == "xmlfile_read" :
        df=read(json_data)
        # logging.info(df.__next__())
        COUNTER=0
        for i in df :
            # logging.info(i)
            COUNTER+=1
            write(json_data, i)
    else:
        logging.info("only ingestion available currently")


    # postcheck script execution starts here
    if json_data["task"]["target"]["target_type"] == 'csv_write' or json_data["task"]["target"]["target_type"] == 'postgres_write' or \
      json_data["task"]["target"]["target_type"] == 'mysql_write' or json_data["task"]["target"]["target_type"] == 'mssql_write' :
        # post check code
        post_check = dq.qc_post_check(json_data, json_checks)

        #qc report generation
        qc_report = dq.qc_report(pre_check, post_check, json_data,new_path)
        logging.info(qc_report)

