""" importing modules """
from configparser import ConfigParser
from datetime import datetime
import multiprocessing
from multiprocessing.pool import ThreadPool
import great_expectations as ge
import logging
import sqlalchemy
import numpy as np
import pandas as pd


def initiate_logging(project, log_loc):
    """Function to initiate log file which will be used across framework"""
    try:
        log_file_nm= project + '.log'
        log_file = log_loc + log_file_nm
        format_str = '%(asctime)s | %(name)-10s | %(processName)-12s | %(funcName)s | %(levelname)-5s | %(message)s'
        formatter = logging.Formatter(format_str)
        logging.basicConfig(filename=log_file, filemode='w', level=logging.INFO, format=format_str)
        st_handler = logging.StreamHandler()   #create handler and set the log level
        st_handler.setLevel(logging.INFO)
        st_handler.setFormatter(formatter)
        logging.getLogger().addHandler(st_handler) 
    except Exception as ex:
        logging.error('UDF Failed: initiate_logging failed')
        raise ex


def read_config(filename, section):
    """reading the postgresql.ini file""" 
    parser = ConfigParser()
    parser.read(filename)
    datb = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            datb[item[0]] = item[1] 
    else:
        raise Exception(f'{0} not found in the {1} file'.format(section, filename))
    return datb  


#logging.info("entered into run_checks_in_parallel function")
def run_checks_in_parallel(index, cols, control_table_df, checks_mapping_df, ge_df):
    """Running all the checks specified in control table in parallel"""
    #logging.info("entered into run_checks_in_parallel function")
    try:
        output_df = pd.DataFrame(
            columns=cols + [
                'unexpected_index_list', 'threshold_voilated_flag', 'run_flag', 'result',
                'output_reference', 'start_time', 'end_time', 'good_records_file', 'bad_records_file',
                'good_records_count', 'bad_records_count'])
        output_df.at[index,'threshold_voilated_flag'] = 'N'
        output_df.at[index, 'run_flag'] = 'N'
        logging.info(f'QC for {control_table_df.at[index, "check"]} check started')
        for col in cols:
            output_df.at[index, col] = control_table_df.at[index, col]
        if control_table_df.at[index, 'active'] == 'Y':
            output_df.at[index, 'run_flag'] = 'Y'
            output_df.at[index, 'start_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            checks_nm = control_table_df.at[index, 'check']
            inputs_required = control_table_df.at[index, 'parameters'].split('|')
            #This section covers all the great expectations QA checks
            func_name = 'expect_' + checks_nm
            default_parameters_dtypes = checks_mapping_df[
                checks_mapping_df['func_name'] == func_name]['parameters'].item().split('|')
            ge_df_func = "ge_df." + func_name + '('
            for i, ele in enumerate(default_parameters_dtypes):
                if ele == 'string':
                    ge_df_func = ge_df_func + "'{" + f"{i}" + "}',"
                else:
                    ge_df_func = ge_df_func + "{" + f"{i}" + "},"
            ge_df_func = ge_df_func + "result_format = 'COMPLETE')"
            ge_df_func = ge_df_func.format(*inputs_required)
            logging.info(f'GE function generated - {ge_df_func}')
            res = eval(ge_df_func)
            output_df.at[index, 'result'] = 'PASS' if res['success'] is True else 'FAIL'
            logging.info(
                f'QC for {control_table_df.at[index,"check"]} check has been {output_df.at[index, "result"]}')
            if not res['success'] and control_table_df.at[index, 'ignore_bad_records'] == 'Y':
                output_df.at[index, 'output_reference'] = res['result']
                if 'unexpected_index_list' in res['result']:
                    output_df.at[
                        index, 'unexpected_index_list'] = res['result']['unexpected_index_list']
                    if control_table_df.at[
                        index, 'threshold_bad_records'] < res['result']['unexpected_percent']:
                        output_df.at[index,'threshold_voilated_flag'] = 'Y'
                else:
                    output_df.at[index, 'unexpected_index_list'] = []
                if type(output_df.at[index, 'unexpected_index_list']) == float:
                    output_df.at[index, 'unexpected_index_list'] = []
                output_df.at[index, 'good_records_count']=ge_df.shape[0] - len(output_df.at[index, 'unexpected_index_list'])
                output_df.at[index, 'bad_records_count']=ge_df.shape[0] - output_df.at[index, 'good_records_count']
                output_df.at[index, 'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")     
        return output_df 
    except Exception as error:
        logging.error("error in run_checks_in_parallel function.")
        raise error   
    

def qc_check(control_table_df, checks_mapping_df, check_type, ing_type, ing_loc, conn_str):
    """Extaracting qc_check related details"""
    #logging.info("entered into qc_check function")
    try:
        control_table_df = control_table_df[control_table_df[
            'type'] == check_type].reset_index(drop=True)
        cols = control_table_df.columns.tolist()
        resultset = pd.DataFrame(
            columns=cols + [
                'unexpected_index_list', 'threshold_voilated_flag', 'run_flag', 'result','output_reference', 
                'start_time', 'end_time', 'good_records_file', 'bad_records_file', 
                'good_records_count', 'bad_records_count'])
        row_val = control_table_df.index.values.tolist()
        pool = ThreadPool(multiprocessing.cpu_count())
        #Creating conditions for different file formats
        if ing_type == 'csv_read' :
            ge_df = ge.read_csv(ing_loc, encoding='utf-8')
            Tot_no_of_records_1 = ge_df.shape
            logging.info(f'Reading csv file started at {ing_loc}')
            logging.info(f'Total number of records present in above path are {Tot_no_of_records_1}')
        if ing_type == 'parquet':
            ge_df = ge.read_parquet(ing_loc)
            Tot_no_of_records_2 = ge_df.shape
            logging.info(f'Reading parquet file started at {ing_loc}')
            logging.info(f'Total number of records present in above path are {Tot_no_of_records_2}')
        if ing_type == 'json':
            ge_df = ge.read_json(ing_loc)
            Tot_no_of_records_3 = ge_df.shape
            logging.info(f'Reading json file started at {ing_loc}')
            logging.info(f'Total number of records present in above path are {Tot_no_of_records_3}')
        if ing_type == 'excel':
            ge_df = ge.read_excel(ing_loc)
            Tot_no_of_records_4 = ge_df.shape
            logging.info(f'Reading xlsx file started at {ing_loc}')
            logging.info(f'Total number of records present in above path are {Tot_no_of_records_4}')
        if ing_type == 'xml':
            pd_df = pd.read_xml(ing_loc)
            ge_df = ge.from_pandas(pd_df)
            Tot_no_of_records_5 = ge_df.shape
            logging.info(f'Reading xlsx file started at {ing_loc}')
            logging.info(f'Total number of records present in above path are {Tot_no_of_records_5}')
        elif ing_type == 'postgres_write' or ing_type == 'postgres_read':
            conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
            pd_df = pd.read_sql(f'select * from {ing_loc}', conn)
            ge_df = ge.from_pandas(pd_df)
        #else:
            #raise Exception("Not a valid ingestion type")
        datasets = pool.map(
            lambda x:run_checks_in_parallel(
                x, cols, control_table_df, checks_mapping_df, ge_df), row_val)
        pool.close()
        for x in datasets:
            resultset = pd.concat([resultset, x])       
        resultset['unexpected_index_list'] = resultset['unexpected_index_list'].replace(np.nan,'')
        bad_records_indexes = list(set([
            item for sublist in resultset['unexpected_index_list'].tolist() for item in sublist]))
        output_loc = ing_type.strip('.csv') + "_"
        if check_type == 'pre_check':
            if 'FAIL' in resultset.result.values:
                indexes = list(set(bad_records_indexes))
                bad_records_df = ge_df[ge_df.index.isin(indexes)]
                No_of_bad_records = bad_records_df.shape[0]
                logging.info(f'Total number of bad records are {No_of_bad_records}')
                bad_records_df.to_csv(
                    output_loc + 'rejected_records_' + datetime.now().strftime(
                        "%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
                if 'Y' in resultset['threshold_voilated_flag']:
                    good_records_df = pd.DataFrame(columns=ge_df.columns.tolist())
                    good_records_df.to_csv(
                        output_loc + 'accepted_records_' + datetime.now().strftime(
                            "%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
                else:
                    good_records_df = ge_df[~ge_df.index.isin(indexes)]
                    No_of_good_records = good_records_df.shape[0]
                    logging.info(f'Total number of good records are {No_of_good_records}')
                    good_records_df.to_csv(
                        output_loc + 'accepted_records_' + datetime.now().strftime(
                            "%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
            else:
                good_records_df = ge_df
                good_records_df.to_csv(
                    output_loc + 'accepted_records_' + datetime.now().strftime(
                        "%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
            resultset[
                'good_records_file'] = output_loc + 'accepted_records_' + datetime.now().strftime(
                "%d_%m_%Y_%H_%M_%S") + '.csv'
            resultset[
                'bad_records_file'] = output_loc + 'rejected_records_' + datetime.now().strftime(
                "%d_%m_%Y_%H_%M_%S") + '.csv'
        else:
            resultset['good_records_file'] = ""
            resultset['bad_records_file'] = ""
        resultset = resultset.drop(columns = ['unexpected_index_list', 'threshold_voilated_flag'])
        return resultset
    except Exception as error:
        logging.error("error in qc_check function.")
        raise error


def qc_pre_check(config_json_file):
    """Function to perform pre_check operation"""
    try:
        #To read configure json file to extract important deatils
        control_table = pd.DataFrame(config_json_file['task']['data_quailty'])
        #control_table = config_json_file['task']['data_quailty']
        #Creating connection to postgresql by using sqlalchemy
        conn_str = read_config(
            config_json_file['task']['config_file'], config_json_file['task']['target']['connection_name'])
        conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
        #Reading postgresql checks_mapping table
        checks_mapping = pd.read_sql("select * from checks_mapping", conn)
        src_conn_str =  read_config(
            config_json_file['task']['config_file'], config_json_file['task']['source'][
                'connection_name']) if config_json_file['task']['source']['connection_name'] != '' else ''
        logging.info("Pre_check operation started")
        if config_json_file['task']['source']['source_type'] =='csv_read':
            config=config_json_file['task']['source']['source_file_path']+config_json_file['task']['source']['source_file_name']+'.csv'
            print(config)
        elif config_json_file['task']['source']['source_type'] =='postgres_read':
            config=config_json_file['task']['source']['table_name']

        print(config_json_file['task']['source']['source_file_path'])
        pre_check_result = qc_check(
            control_table, checks_mapping, 'pre_check', config_json_file['task']['source'][
                'source_type'], config, src_conn_str)
        pre_check_result.to_csv('pre_check_result' + datetime.now().strftime(
            "%d_%m_%Y_%H_%M_%S") + '.csv',index=False)
        logging.info("Pre_check operation completed")
        return pre_check_result
    except Exception as error:
        logging.error("error in qc_pre_check function.")
        raise error


def qc_post_check(config_json_file):
    """Function to perform post_check operation"""
    try:
        control_table = pd.DataFrame(config_json_file['task']['data_quailty'])
        #print(control_table)
        #Creating connection to postgresql by using sqlalchemy
        conn_str = read_config(
            config_json_file['task']['config_file'], config_json_file['task']['target']['connection_name'])
        conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
        #Reading postgresql checks_mapping table
        checks_mapping = pd.read_sql("select * from checks_mapping", conn)
        tgt_conn_str =  read_config(
            config_json_file['task']['config_file'], config_json_file['task']['target'][
                'connection_name']) if config_json_file['task']['target']['connection_name'] != '' else ''
        logging.info("Post_check operation started")
        post_check_result = qc_check(
            control_table, checks_mapping, 'post_check', config_json_file['task']['target'][
                'target_type'], config_json_file['task']['target']['table_name'], tgt_conn_str)
        logging.info("Post_check operation completed")
        return post_check_result
    except Exception as error:
        logging.error("error in qc_post_check function.")
        raise error


def qc_report(pre_check_result, post_check_result):
    """Function to generate qc_report"""
    try:
        #Concatinating the pre_check and post_check results into final_check_result
        final_check_result = pd.concat([pre_check_result, post_check_result], axis=0)
        final_check_result = final_check_result.reset_index(drop=True)
        final_check_result.to_csv('qc_report_' + datetime.now().strftime(
            "%d_%m_%Y_%H_%M_%S") + '.csv',index=False)
        logging.info("qc_report generated")
        return final_check_result
    except Exception as error:
        logging.error("error in qc_report function.")
        raise error
