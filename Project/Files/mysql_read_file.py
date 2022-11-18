""" importing modules """
import json
import sqlalchemy
import logging
import pandas as pd
from utility import establish_mysql_conn


def read(json_data: dict) -> bool:
    """ function for reading data from postgres table"""
    try:
        # logging.info("%s is the source connection name", json_data["task"]["source"]["connection_name"])
        # creating source connection to postgres
        print("hell")
        conn3 = establish_mysql_conn(json_data, 'source','source_file_path')
        print("hell")
        print(conn3)
        logging.info("%s connection to postgres established", json_data["task"]["source"]["connection_name"])
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




#test
# try:
#     with open(r"C:/Users/ParulShrikhande/Desktop/Latest_code/mysql.json",'r', encoding='utf-8') as jsonfile:
#         print("reading json data started")
#         json_data = json.load(jsonfile)
#         print("reading json data completed")
# except Exception as error:
#     logging.exception("error in reading json %s.", str(error))
#     raise Exception("error in reading json: " + str(error)) from error

# read(json_data)