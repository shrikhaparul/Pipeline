
from github import Github
import json
import subprocess


def download_code(Project_id,Task_id):
    g = Github("shrikhaparul", "infotech@1_1")
    
    dwn_json='curl -o C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\test\\'+Task_id+'.json '\
    'https://raw.githubusercontent.com/shrikhaparul/test/main/Project/'+Project_id+'/Task/'+Task_id+'.json'
    #r='curl -o C:\\Users\\ParulShrikhande\\Desktop\\Database_ingestion_kart\\main_9.py mapping_1.decoded_content.decode()'
    subprocess.call(dwn_json)
    
    with open(
        r"C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\"+Task_id+".json",
        "r", encoding='utf-8') as jsonfile:
        config_json = json.load(jsonfile)
    source_conn_file = config_json['task']['source']['connection_name']
    target_conn_file = config_json['task']['target']['connection_name']
    source_type = config_json['task']['source']['source_type']
    target_type = config_json['task']['target']['target_type']
    print(target_type)
    
    #curl command for downloading the files
    src_ini = 'curl -o C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\test\\'+source_conn_file+' '\
    'https://raw.githubusercontent.com/shrikhaparul/test/main/Project/'+Project_id+'/'+source_conn_file
    trgt_ini = 'curl -o C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\test\\'+target_conn_file+' '\
    'https://raw.githubusercontent.com/shrikhaparul/test/main/Project/'+Project_id+'/'+target_conn_file

    
    #connecting with github
    g = Github("shrikhaparul", "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")
    repo = g.get_user().get_repo('intelli_kart')
    
    #reading git hub data
    mapping =repo.get_contents("mapping.json")
    
    print(mapping)
    print(target_type)
    
    y= mapping.decoded_content.decode()
    
    data = json.loads(y)
    
    print(target_type)
    print(data[source_type])
    source_file_name=(data[source_type])
    target_file_name=(data[target_type])
    print(target_type)
    #curl command for downloading the files
    src_py = 'curl -o C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\test\\'+source_file_name+' '\
    'https://raw.githubusercontent.com/shrikhaparul/test/main/Project/Files/'+source_file_name
    trgt_py = 'curl -o C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\test\\'+target_file_name+' '\
    'https://raw.githubusercontent.com/shrikhaparul/test/main/Project/Files/'+target_file_name
    
    #calling the subprocess to run curl command function
    subprocess.call(src_ini)
    subprocess.call(trgt_ini)
    subprocess.call(src_py)
    subprocess.call(trgt_py)
     

Project_id= 'P_555'
Task_id = 'T_7866'
download_code(Project_id,Task_id)

