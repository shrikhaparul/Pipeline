""" importing modules """
import json
import datetime
import sqlalchemy
import pandas as pd
import definitions_qc as dq
dq.initiate_logging(
    'log_qc_check', r'C:\\Users\\ParulShrikhande\\Desktop\\Latest_code\\')


try:
    with open(
        r"C:\\Users\\ParulShrikhande\\Desktop\\Latest_code\\1234.json",
        "r", encoding='utf-8') as jsonfile:
            dq.logging.info("reading json data")
            config_json = json.load(jsonfile)
            dq.logging.info("reading json data completed")
except Exception as error:
    dq.logging.error("error in reading json %s.", str(error))
    raise Exception("error in reading json: " + str(error)) from error


"""def qc_pre_check(config_json_file):
    #Function to perform pre_check operation
    try:
        #To read configure json file to extract important deatils
        control_table = pd.read_json(config_json_file['control_table'])
        #Creating connection to postgresql by using sqlalchemy
        conn_str = dq.read_config(config_json_file['config_path'], config_json_file['config_section_nm'])
        conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
        #Reading postgresql checks_mapping table
        checks_mapping = pd.read_sql("select * from checks_mapping", conn)
        src_conn_str =  dq.read_config(
            config_json_file['config_path'], config_json_file[
                'src_section_nm']) if config_json_file['src_section_nm'] != '' else ''
        dq.logging.info("Pre_check operation started")
        pre_check_result = dq.qc_check(
            control_table, checks_mapping, 'pre_check', config_json_file[
                'source'], config_json_file['src_loc'], src_conn_str)
        dq.logging.info("Pre_check operation completed")
        return pre_check_result
    except Exception as error:
        dq.logging.error("error in qc_pre_check function.")
        raise error"""
pre_check = dq.qc_pre_check(config_json)


"""def qc_post_check(config_json_file):
    #Function to perform post_check operation
    try:
        control_table = pd.read_json(config_json_file['control_table'])
        #Creating connection to postgresql by using sqlalchemy
        conn_str = dq.read_config(config_json_file['config_path'], config_json_file['config_section_nm'])
        conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
        #Reading postgresql checks_mapping table
        checks_mapping = pd.read_sql("select * from checks_mapping", conn)
        tgt_conn_str =  dq.read_config(
            config_json_file['config_path'], config_json_file[
                'tgt_section_nm']) if config_json_file['tgt_section_nm'] != '' else ''
        dq.logging.info("Post_check operation started")
        post_check_result = dq.qc_check(
            control_table, checks_mapping, 'post_check', config_json_file[
                'target'], config_json_file['tgt_loc'], tgt_conn_str)
        dq.logging.info("Post_check operation completed")
        return post_check_result
    except Exception as error:
        dq.logging.error("error in qc_post_check function.")
        raise error"""
post_check = dq.qc_post_check(config_json)


"""def qc_report(pre_check_result, post_check_result):
    #Function to generate qc_report
    try:
        #Concatinating the pre_check and post_check results into final_check_result
        final_check_result = pd.concat([pre_check_result, post_check_result], axis=0)
        final_check_result = final_check_result.reset_index(drop=True)
        final_check_result.to_csv('qc_report_' + datetime.now().strftime(
            "%d_%m_%Y_%H_%M_%S") + '.csv',index=False)
        dq.logging.info("qc_report generated")
    except Exception as error:
        dq.logging.error("error in qc_report function.")
        raise error"""
qc_report = dq.qc_report(pre_check, post_check)
print(qc_report)