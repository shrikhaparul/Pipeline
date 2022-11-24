""" importing modules """
from datetime import datetime
import multiprocessing
from multiprocessing.pool import ThreadPool
import logging
import json
import os
import glob
import great_expectations as ge
import sqlalchemy
import numpy as np
import pandas as pd


def initiate_logging(project, log_loc):
    """Function to initiate log file which will be used across framework"""
    try:
        log_file_nm= project + '.log'
        log_file = log_loc + log_file_nm
        format_str = '%(asctime)s | %(name)-10s | %(processName)-12s | %(funcName)s |\
         %(levelname)-5s | %(message)s'
        formatter = logging.Formatter(format_str)
        logging.basicConfig(filename=log_file, filemode='w', level=logging.INFO, format=format_str)
        st_handler = logging.StreamHandler()   #create handler and set the log level
        st_handler.setLevel(logging.INFO)
        st_handler.setFormatter(formatter)
        logging.getLogger().addHandler(st_handler)
        logging.info("Logging Initiated")
    except Exception as ex:
        logging.error('UDF Failed: initiate_logging failed')
        raise ex


def get_config_section(config_path:str, conn_nm: str) -> dict:
    """reads the connection file and returns connection details as dict for
       connection name you pass it through the json
    """
    try:
        with open(config_path,'r', encoding='utf-8') as jsonfile:
            #logging.info("fetching connection details")
            json_data = json.load(jsonfile)
            #logging.info("reading connection details completed")
            # print("connection established")
            return dict(json_data[conn_nm].items())
    except Exception as error:
        logging.exception("get_config_section() is %s.", str(error))
        raise Exception("get_config_section(): " + str(error)) from error   


# def read_config(filename, section):
#     """reading the postgresql.ini file"""
#     parser = ConfigParser()
#     parser.read(filename)
#     datb = {}
#     if parser.has_section(section):
#         items = parser.items(section)
#         for item in items:
#             datb[item[0]] = item[1]
#     else:
#         raise Exception(f'{0} not found in the {1} file'.format(section, filename))
#     return datb


def run_checks_in_parallel(index, cols, control_table_df, checks_mapping_df, ge_df):
    """Running all the checks specified in control table in parallel"""
    try:
        output_df = pd.DataFrame(
            columns=cols + [
                'unexpected_index_list', 'threshold_voilated_flag', 'run_flag', 'result',
                'output_reference', 'start_time', 'end_time', 'good_records_file',
                 'bad_records_file', 'good_records_count', 'bad_records_count'])
        output_df.at[index,'threshold_voilated_flag'] = 'N'
        output_df.at[index, 'run_flag'] = 'N'
        logging.info('QC for %s check started', control_table_df.at[index, "check"])
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
            logging.info('GE function generated - %s', ge_df_func)
            res = eval(ge_df_func)
            output_df.at[index, 'result'] = 'PASS' if res['success'] is True else 'FAIL'
            logging.info('QC for %s check has been %s', control_table_df.at[
                index,"check"], output_df.at[index, "result"])
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
                output_df.at[index, 'good_records_count']=ge_df.shape[0] - len(
                    output_df.at[index, 'unexpected_index_list'])
                output_df.at[
                index, 'bad_records_count']=ge_df.shape[0] - output_df.at[
                    index, 'good_records_count']
                output_df.at[index, 'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return output_df
    except Exception as error:
        logging.exception("error in run_checks_in_parallel function %s.", str(error))
        raise Exception("error in run_checks_in_parallel function:" + str(error)) from error


def qc_check(control_table_df, checks_mapping_df, check_type, project_id, ing_type,
ing_loc, src_loc, tgt_loc, ing_encoding, ing_sheetnum, conn_str, dq_output_loc=None):
    """Extaracting qc_check related details"""
    #logging.info("entered into qc_check function")
    try:
        control_table_df = control_table_df[control_table_df[
            'type'] == check_type].reset_index(drop=True)
        cols = control_table_df.columns.tolist()
        resultset = pd.DataFrame(
            columns=cols + [
                'unexpected_index_list', 'threshold_voilated_flag', 'run_flag', 'result',
                'output_reference', 'start_time', 'end_time', 'good_records_file',
                 'bad_records_file', 'good_records_count', 'bad_records_count'])
        row_val = control_table_df.index.values.tolist()
        pool = ThreadPool(multiprocessing.cpu_count())
        #Creating conditions for different file formats
        if ing_type == 'csv_read':
            ge_df = ge.read_csv(ing_loc, encoding=ing_encoding)
            shape_of_records1 = ge_df.shape
            logging.info('Reading csv file started at %s', ing_loc)
            logging.info(
            'Total number of records present in above path are %s', shape_of_records1)
        elif ing_type == 'csv_write':
            ge_df = ge.read_csv(tgt_loc, encoding=ing_encoding)
            shape_of_records2 = ge_df.shape
            logging.info('Reading csv file started at %s', tgt_loc)
            logging.info(
            'Total number of records present in above path are %s', shape_of_records2)
        elif ing_type == 'parquet':
            ge_df = ge.read_parquet(ing_loc)
            shape_of_records3 = ge_df.shape
            logging.info('Reading parquet file started at %s', ing_loc)
            logging.info(
                'Total number of records present in above path are %s', shape_of_records3)
        elif ing_type == 'json':
            ge_df = ge.read_json(ing_loc, encoding=ing_encoding)
            shape_of_records4 = ge_df.shape
            logging.info('Reading json file started at %s', ing_loc)
            logging.info(
                'Total number of records present in above path are %s', shape_of_records4)
        elif ing_type == 'excel':
            ge_df = ge.read_excel(ing_loc, sheet_name=ing_sheetnum)
            shape_of_records5 = ge_df.shape
            logging.info('Reading excel file started at %s', ing_loc)
            logging.info(
                'Total number of records present in above path are %s', shape_of_records5)
        elif ing_type == 'xml':
            pd_df = pd.read_xml(ing_loc)
            ge_df = ge.from_pandas(pd_df)
            shape_of_records6 = ge_df.shape
            logging.info('Reading xml file started at %s', ing_loc)
            logging.info(
                'Total number of records present in above path are %s', shape_of_records6)
        elif ing_type == 'postgres_read': 
            conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
            pd_df = pd.read_sql(f'select * from {src_loc}', conn)
            ge_df = ge.from_pandas(pd_df)
            shape_of_records7 = ge_df.shape
            logging.info('Reading postgres db started at %s table', src_loc)
            logging.info(
                'Total number of records present in above table are %s', shape_of_records7)
        elif ing_type == 'postgres_write': 
            conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
            pd_df = pd.read_sql(f'select * from {ing_loc}', conn)
            ge_df = ge.from_pandas(pd_df)
            shape_of_records8 = ge_df.shape
            logging.info('Reading postgres db started at %s table', ing_loc)
            logging.info(
                'Total number of records present in above table are %s', shape_of_records8)
        elif ing_type == 'mysql_read':
            conn = sqlalchemy.create_engine(f'mysql://{conn_str["user"]}'
            f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}'
            f':{int(conn_str["port"])}/{conn_str["database"]}', encoding='utf-8')
            pd_df = pd.read_sql(f'select * from {src_loc}', conn)
            ge_df = ge.from_pandas(pd_df)
            shape_of_records9 = ge_df.shape
            logging.info('Reading mysql db started at %s table', src_loc)
            logging.info(
                'Total number of records present in above table are %s', shape_of_records9)
        elif ing_type == 'mysql_write':
            conn = sqlalchemy.create_engine(f'mysql://{conn_str["user"]}'
            f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}'
            f':{int(conn_str["port"])}/{conn_str["database"]}', encoding='utf-8')
            pd_df = pd.read_sql(f'select * from {ing_loc}', conn)
            ge_df = ge.from_pandas(pd_df)
            shape_of_records10 = ge_df.shape
            logging.info('Reading mysql db started at %s table', ing_loc)
            logging.info(
                'Total number of records present in above table are %s', shape_of_records10)
        #else:
            #raise Exception("Not a valid ingestion type")
        datasets = pool.map(
            lambda x:run_checks_in_parallel(
                x, cols, control_table_df, checks_mapping_df, ge_df), row_val)
        pool.close()
        for datas in datasets:
            resultset = pd.concat([resultset, datas])
        resultset['unexpected_index_list'] = resultset['unexpected_index_list'].replace(np.nan,'')
        bad_records_indexes = list({
            item for sublist in resultset['unexpected_index_list'].tolist() for item in sublist})
        if 'FAIL' in resultset.result.values:
            indexes = list(set(bad_records_indexes))
            bad_records_df = ge_df[ge_df.index.isin(indexes)]
            no_of_bad_records = bad_records_df.shape[0]
            logging.info('Total number of bad records are %s', no_of_bad_records)
            bad_records_df.to_csv(
                dq_output_loc + project_id +'_'+ check_type + '_rejected_records_' +
                 datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
            if 'Y' in resultset['threshold_voilated_flag']:
                good_records_df = pd.DataFrame(columns=ge_df.columns.tolist())
                good_records_df.to_csv(
                    dq_output_loc + project_id+'_'+ check_type +'_accepted_records_' +
                    datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
            else:
                good_records_df = ge_df[~ge_df.index.isin(indexes)]
                no_of_good_records = good_records_df.shape[0]
                logging.info('Total number of good records are %s', no_of_good_records)
                good_records_df.to_csv(
                    dq_output_loc + project_id+'_'+ check_type + '_accepted_records_' +
                    datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
        else:
            good_records_df = ge_df
            good_records_df.to_csv(
                dq_output_loc + project_id+'_'+ check_type + '_accepted_records_' +
                datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
        resultset[
            'good_records_file'] = dq_output_loc+ project_id+\
            '_'+ check_type +'_accepted_records_' + datetime.now().strftime(
            "%d_%m_%Y_%H_%M_%S") + '.csv'
        resultset[
            'bad_records_file'] = dq_output_loc+ project_id+\
            '_'+ check_type +'_rejected_records_' + datetime.now().strftime(
            "%d_%m_%Y_%H_%M_%S") + '.csv'
        resultset = resultset.drop(
        columns = ['unexpected_index_list', 'threshold_voilated_flag'])
        return resultset
    except Exception as error:
        logging.exception("error in qc_check function %s.", str(error))
        raise Exception("error in qc_check function:" + str(error)) from error


def qc_pre_check(config_json_file):
    """Function to perform pre_check operation"""
    try:
        #To read configure json file to extract important deatils
        control_table = pd.DataFrame(config_json_file['task']['data_quality'])
        #Creating connection to postgresql by using sqlalchemy
        conn_str = get_config_section(
            config_json_file['task']['config_file'], 
            config_json_file['task']['config_conn_name'])
        conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
        #Reading postgresql checks_mapping table
        checks_mapping = pd.read_sql("select * from checks_mapping", conn)
        src_conn_str =  get_config_section(
            config_json_file['task']['source']['connection_file_path'],
            config_json_file[
                'task']['source']['connection_name']) if config_json_file[
                'task']['source']['connection_name'] != '' else ''
        logging.info("Pre_check operation started")
        output_loc = config_json_file[
            'task']['data_quality_files']['acptd_and_rjctd_file_path']
        # Reprocessing of bad records file
        if config_json_file['task']['source'][
            'updated_rejected_records'] == '' or config_json_file[
            'task']['source']['updated_rejected_records'] == 'N':
            pre_check_result = qc_check(
                control_table, checks_mapping, 'pre_check',config_json_file[
                    'project_id'], config_json_file['task']['source']['source_type'],
                    config_json_file['task']['source']['source_file_path']+config_json_file[
                        'task']['source']['source_file_name']+'.csv',config_json_file[
                            'task']['source']['table_name'],
                    config_json_file['task']['target']['connection_file_path']+config_json_file[
                            'task']['target']['table_name']+'.csv',
                    config_json_file['task']['source']['encoding'],
                    config_json_file['task']['source']['sheet_name'],
                    src_conn_str, output_loc)
        elif config_json_file['task']['source']['updated_rejected_records'] == 'Y':
            list_of_files = glob.glob(
                'C:\\Users\\PuneethS\\Desktop\\A & H Infotech\\Testing\\project\\*.csv')
            #print(list_of_files)
            latest_file = max(list_of_files, key=os.path.getctime, default='None')
            #print(latest_file)
            reprocessing_source_type = latest_file.split('.')[1]
            #print(reprocessing_source_type)
            pre_check_result = qc_check(
                control_table, checks_mapping, 'pre_check',config_json_file[
                    'project_id'], reprocessing_source_type, latest_file,
                     config_json_file['task']['source']['encoding'],
                     config_json_file['task']['source']['sheet_name'],
                     src_conn_str, output_loc)
        logging.info("Pre_check operation completed")
        # pre_check_result.to_csv('pre_check_result' + datetime.now().strftime(
        #     "%d_%m_%Y_%H_%M_%S") + '.csv',index=False)
        return pre_check_result
    except Exception as error:
        logging.exception("error in qc_pre_check function %s.", str(error))
        raise Exception("error in qc_pre_check function:" + str(error)) from error


def qc_post_check(config_json_file):
    """Function to perform post_check operation"""
    try:
        control_table = pd.DataFrame(config_json_file['task']['data_quality'])
        #Creating connection to postgresql by using sqlalchemy
        conn_str = get_config_section(
            config_json_file['task']['config_file'], 
            config_json_file['task']['config_conn_name'])
        conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
        #Reading postgresql checks_mapping table
        checks_mapping = pd.read_sql("select * from checks_mapping", conn)
        tgt_conn_str =  get_config_section(
            config_json_file['task']['target']['connection_file_path'],
            config_json_file['task']['target']['connection_name']) if config_json_file[
                  'task']['target']['connection_name'] != '' else ''
        logging.info("Post_check operation started")
        output_loc = config_json_file['task'][
                  'data_quality_files']['acptd_and_rjctd_file_path']
        # Reprocessing of bad records file
        if config_json_file['task']['source'][
            'updated_rejected_records'] == '' or config_json_file[
            'task']['source']['updated_rejected_records'] == 'N':
            post_check_result = qc_check(
                control_table, checks_mapping, 'post_check', config_json_file['project_id'],
                config_json_file['task']['target']['target_type'], config_json_file[
                        'task']['target']['table_name'], config_json_file['task'][
                            'source']['table_name'],config_json_file['task']['target'][
                                'connection_file_path']+config_json_file[
                            'task']['target']['table_name']+'.csv', config_json_file[
                        'task']['target']['encoding'], config_json_file[
                        'task']['source']['sheet_name'], tgt_conn_str, output_loc)
        elif config_json_file['task']['source']['updated_rejected_records'] == 'Y':
            list_of_files = glob.glob(
                'C:\\Users\\PuneethS\\Desktop\\A & H Infotech\\Testing\\project\\*.csv')
            latest_file = max(list_of_files, key=os.path.getctime, default='None')
            reprocessing_source_type = latest_file.split('.')[1]
            post_check_result = qc_check(
                control_table, checks_mapping, 'post_check', config_json_file['project_id'],
                reprocessing_source_type, latest_file,config_json_file[
                        'task']['source']['encoding'], config_json_file[
                        'task']['source']['sheet_name'], tgt_conn_str, output_loc)
        logging.info("Post_check operation completed")
        return post_check_result
    except Exception as error:
        logging.error("error in qc_post_check function %s.", str(error))
        raise Exception("error in qc_post_check function:" + str(error)) from error


def qc_report(pre_check_result, post_check_result, config_json_file):
    """Function to generate qc_report"""
    try:
        #Concatinating the pre_check and post_check results into final_check_result
        output_loc = config_json_file['task'][
                  'data_quality_files']['qc_report_file_path']
        final_check_result = pd.concat([pre_check_result, post_check_result], axis=0)
        final_check_result = final_check_result.reset_index(drop=True)
        final_check_result.to_csv(output_loc + 'qc_report_' + datetime.now().strftime(
            "%d_%m_%Y_%H_%M_%S") + '.csv',index=False)
        logging.info("qc_report generated")
        return final_check_result
    except Exception as error:
        logging.exception("error in qc_report function %s.", str(error))
        raise Exception("error in qc_report function: " + str(error)) from error
