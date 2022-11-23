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
    x=path+Task_id+".json"
    print(x)
    with open(
        r""+path+Task_id+".json","r", encoding='utf-8') as jsonfile:
        config_json = json.load(jsonfile)
    source_conn_file = config_json['task']['source']['connection_name']
    target_conn_file = config_json['task']['target']['connection_name']
    source_type = config_json['task']['source']['source_type']
    target_type = config_json['task']['target']['target_type']
    src_loc= "D:\\Ingestion_Kart\\Pipeline\\Project\\P_555"
    trg_loc= config_json['task']['target']['target_file_path']
    config_loc= config_json['task']['config_file']
    print(source_conn_file,target_conn_file,source_type,target_type)
    
    
    #curl command for downloading the files
    src_ini = 'curl -o D:\\Ingestion_Kart\\Pipeline\\Project\\P_555\\'+source_conn_file+'.ini '\
    'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/'+Project_id+'/'+source_conn_file+'.ini'
    trgt_ini = 'curl -o D:\\Ingestion_Kart\\Pipeline\\Project\\P_555\\'+target_conn_file+'.ini '\
    'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/'+Project_id+'/'+target_conn_file+'.ini'
    config_ini= 'curl -o D:\\Ingestion_Kart\\Pipeline\\Project\\P_555\\config.ini '\
    'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/'+Project_id+'/'+'config.ini'
    
    subprocess.call(src_ini)
    subprocess.call(trgt_ini)
    subprocess.call(config_ini)
    

    
#     #connecting with github
#     g = Github("shrikhaparul", "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")
#     repo = g.get_user().get_repo('intelli_kart')
    
#     #reading git hub data
#     mapping =repo.get_contents("mapping.json")
    
#     print(mapping)
#     print(target_type)
    
#     y= mapping.decoded_content.decode()
    
#     data = json.loads(y)
  
    mapping_file='curl -o '+path+'mapping.json '\
    'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/Files/mapping.json'

    subprocess.call(mapping_file)

    with open(r""+path+'mapping.json',"r", encoding='utf-8') as mapjson:
        config_new_json = json.load(mapjson)
    
    
    source_file_name=(config_new_json["mapping"][source_type])
    
    target_file_name=(config_new_json["mapping"][target_type])
    QC_check_file=config_new_json["default"]["QC_checks"]
    Utility_file=config_new_json["default"]["Utility"]
    print(QC_check_file,Utility_file)
    
    #curl command for downloading the files
    src_py = 'curl -o '+path+source_file_name+' '\
    'https://raw.githubusercontent.com/shrikhaparul/pipeline/main/Project/Files/'+source_file_name
    trgt_py = 'curl -o '+path+target_file_name+' '\
    'https://raw.githubusercontent.com/shrikhaparul/pipeline/main/Project/Files/'+target_file_name
    QC_py= 'curl -o '+path+QC_check_file+' '\
    'https://raw.githubusercontent.com/shrikhaparul/pipeline/main/Project/Files/'+QC_check_file
    Utility_py= 'curl -o '+path+Utility_file+' '\
    'https://raw.githubusercontent.com/shrikhaparul/pipeline/main/Project/Files/'+Utility_file
    
    #calling the subprocess to run curl command function
    subprocess.call(src_py)
    subprocess.call(trgt_py)
    subprocess.call(QC_py)
    subprocess.call(Utility_py)



def engine_main(Project_id,Task_id,path):
    from utility import initiate_logging
    import definitions_qc as dq
    ## logging module
    run_id='run_id_' + datetime.now().strftime("%Y%m%d%H%M%S%f") 
    # new_path=path+'\\'
    os.chdir(path)
    if not os.path.exists('Run_id'):
        os.makedirs('Run_id')
    new_path= path  +'Run_id\\'+run_id+'\\'
    os.chdir(path  +'Run_id\\')
    os.makedirs(run_id)
    initiate_logging(run_id,r""+new_path)
    # print(new_path)
    logging.info('execution started')
    # initiate_logging('engine_log', r""+path)
    logging.info('logging initiated')
    # reading the json file
    try:
        with open(r""+path+Task_id+".json","r", encoding='utf-8') as jsonfile:
            logging.info("reading json data started")
            json_data = json.load(jsonfile)
            logging.info("reading json data completed")
    except Exception as error:
        logging.exception("error in reading json %s.", str(error))
        raise Exception("error in reading json: " + str(error)) from error

    # # Precheck code
    # pre_check = dq.qc_pre_check(json_data)


    #ingestion code
    if json_data["task"]["source"]["source_type"] == "csv_read":
        from csv_read_file import read
    elif json_data["task"]["source"]["source_type"] == "postgres_read":
        from postgres_read_file import read
    elif json_data["task"]["source"]["source_type"] == "mysql_read":
        from mysql_read import read
    if json_data["task"]["target"]["target_type"] == "postgres_write":
        from postgres_write_file import write
    elif json_data["task"]["target"]["target_type"] == "mysql_write":
        from mysql_write import write


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


    # #post check code
    # post_check = dq.qc_post_check(json_data)

    # #qc report generation
    # qc_report = dq.qc_report(pre_check, post_check)
    # logging.info(qc_report)

