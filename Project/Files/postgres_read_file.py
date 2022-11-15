""" script for reading data from postgres table"""
import logging
import pandas as pd
from utility import establish_src_conn

def read(json_data: dict) -> bool:
    """ function for reading data from postgres table"""
    try:
        # logging.info("%s is the source connection name", json_data["task"]["source"]["connection_name"])
        # creating source connection to postgres
        conn3 = establish_src_conn(json_data, 'source')
        # logging.info("%s connection to postgres established", json_data["task"]["source"]["connection_name"])
        logging.info('reading data from postgres started')
        default_columns = None if json_data["task"]["source"]["columns"]==" " else list(json_data["task"]["source"]["columns"]
        .split(","))
        count1 = 0
        for query in pd.read_sql_table(
            json_data["task"]["source"]["table_name"], conn3, json_data["task"]["source"]["schema"],
            columns = default_columns, chunksize = json_data["task"]["source"]["chunk_size"]):
            count1+=1
            logging.info('%s iteration' , str(count1))
            yield query
        conn3.dispose()
        return True
    except Exception as error:
        logging.exception("read_data_from_postgres() is %s", str(error))
        raise Exception("read_data_from_postgres(): " + str(error)) from error