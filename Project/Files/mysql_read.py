""" importing modules """
import logging
import sqlalchemy
import pandas as pd
from utility import get_config_section

def establish_conn(json_data: dict, json_section: str) -> bool:
    """establishes connection for the mysql database
       you pass it through the json"""
    try:
        connection_details = get_config_section(json_data["task"][json_section]["connection_file_path"],\
        json_data["task"][json_section]["connection_name"])
        conn1 = sqlalchemy.create_engine(f'mysql://{connection_details["user"]}'
        f':{connection_details["password"].replace("@", "%40")}@{connection_details["host"]}'
        f':{int(connection_details["port"])}/{connection_details["database"]}', encoding='utf-8')
        # logging.info("connection established")
        return conn1
    except Exception as error:
        logging.exception("establish_conn() is %s", str(error))
        raise Exception("establish_conn(): " + str(error)) from error

def read(json_data: dict) -> bool:
    """ function for reading data from mysql table"""
    try:
        conn3 = establish_conn(json_data, 'source')
        logging.info('reading data from mysql started')
        count1 = 0
        if json_data["task"]["source"]["query"] == " ":
            logging.info("reading from mysql table: %s",
            json_data["task"]["source"]["table_name"])
            default_columns = None if json_data["task"]["source"]["select_columns"]==" "\
            else list(json_data["task"]["source"]["select_columns"].split(","))
            for query in pd.read_sql_table(json_data["task"]["source"]["table_name"], conn3,\
            columns = default_columns, \
            chunksize = json_data["task"]["source"]["chunk_size"]):
                count1+=1
                logging.info('%s iteration' , str(count1))
                yield query
        else:
            logging.info("reading from sql query")
            logging.info('sql_query: %s',json_data["task"]["source"]["query"])
            for query in pd.read_sql(json_data["task"]["source"]["query"],
            conn3, chunksize = json_data["task"]["source"]["chunk_size"]):
                count1+=1
                logging.info('%s iteration' , str(count1))
                yield query
        conn3.dispose()
        return True
    except Exception as error:
        logging.exception("read_data_from_mysql() is %s", str(error))
        raise Exception("read_data_from_mysql(): " + str(error)) from error
