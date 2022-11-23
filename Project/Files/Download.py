import logging
from datetime import datetime
import sys
import os
from github import Github
import json
import subprocess


def download_engine(Project_id,Task_id,path):
    
   os.chdir(path)
   if not os.path.exists('Ingestion_Kart'):
      os.makedirs('Ingestion_Kart')
   os.chdir(path+'Ingestion_Kart\\')
   if not os.path.exists('Pipeline'):
      os.makedirs('Pipeline')
   os.chdir(path+'Ingestion_Kart\\Pipeline\\')
   if not os.path.exists('Project'):
      os.makedirs('Project')
   os.chdir(path+'Ingestion_Kart\\Pipeline\\Project\\')
   if not os.path.exists(Project_id):
      os.makedirs(Project_id)
   os.chdir(path+'Ingestion_Kart\\Pipeline\\Project\\'+Project_id+'\\')
   if not os.path.exists('Task'):
      os.makedirs('Task')
   os.chdir(path+'Ingestion_Kart\\Pipeline\\Project\\'+Project_id+'\\Task\\')
   if not os.path.exists(Task_id):
        os.makedirs(Task_id)
   engine_path= path+'Ingestion_Kart\\Pipeline\\Project\\'+Project_id+'\\Task\\'+Task_id+'\\'

   engine='curl -o '+engine_path+'engine_code.py '\
   'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/Files/engine_code.py'
    #r='curl -o C:\\Users\\ParulShrikhande\\Desktop\\Database_ingestion_kart\\main_9.py mapping_1.decoded_content.decode()'
   subprocess.call(engine)

    # engine_path= path+Task_id+'\\'
    # os.chdir(path)
    # if not os.path.exists(Task_id):
    # os.makedirs(Task_id)
    


def download_json(Project_id,Task_id,path):
    engine_path= path+'Ingestion_Kart\\Pipeline\\Project\\'+Project_id+'\\Task\\'+Task_id+'\\'
    
    dwn_json='curl -o '+engine_path+Task_id+'.json '\
    'https://raw.githubusercontent.com/shrikhaparul/Pipeline/main/Project/'+Project_id+'/Task/'+Task_id+'.json'
    #r='curl -o C:\\Users\\ParulShrikhande\\Desktop\\Database_ingestion_kart\\main_9.py mapping_1.decoded_content.decode()'
    subprocess.call(dwn_json)
    

def initiate_logging(project: str, log_loc: str) -> bool:
    """Function to initiate log file which will be used across framework"""
    try:
        log_file_nm = project + '.log'
        new_file1 = log_loc + log_file_nm
        # create formatter & how we want ours logs to be formatted
        format_str = '%(asctime)s | %(name)-10s | %(processName)-12s |\
            %(funcName)s  |%(levelname)-5s | %(message)s'
        formatter = logging.Formatter(format_str)
        logging.basicConfig(filename=new_file1, filemode='w',
                            level=logging.INFO, format=format_str)
        st_handler = logging.StreamHandler()  # create handler and set the log level
        st_handler.setLevel(logging.INFO)
        st_handler.setFormatter(formatter)
        logging.getLogger().addHandler(st_handler)
        return True
    except Exception as ex:
        logging.error('UDF Failed: initiate_logging failed')
        raise ex










# Run_id='run_id_' + datetime.now().strftime("%Y%m%d%H%M%S%f") 
# Project_id='P_555'
# Task_id='mysql'
# path="C:\\Users\\ParulShrikhande\\Desktop\\Latest_code\\test"
# new_path=path+'\\'+Run_id +'\\'
# os.chdir(path)
# os.makedirs(Run_id)
# initiate_logging(Run_id,r""+new_path)
# # print(new_path)
# logging.info('execution started')


# download_code(Project_id,Task_id,new_path)
# x=Run_id+'.engine_code'
# os.chdir(new_path)
# print(os.getcwd())

def execute_engine(Project_id,Task_id,Path):
    # adding Folder_2/subfolder to the system path
    new_path=Path+'Ingestion_Kart\\Pipeline\\Project\\'+Project_id+'\\Task\\'+Task_id+'\\'
    sys.path.insert(0, new_path)

    import engine_code

    
    engine_code.download_files(Project_id,Task_id,Path)
    engine_code.engine_main(Project_id,Task_id,new_path)
