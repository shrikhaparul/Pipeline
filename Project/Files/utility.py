""" importing modules """
import configparser
import logging
import sqlalchemy
import pandas as pd

# custom log function for framework
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

# reading the cofig.ini file and passing the connection details as dictionary
def get_config_section(config_path: str, conn_nm: str) -> dict:
    """reads the cofig file and returns connection details as dict for
       connection name you pass it through the json
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        if not config.has_section(conn_nm):
            logging.exception("Invalid Connection %s.", str(conn_nm))
            raise Exception(f"Invalid Connection {conn_nm}")
        # print("connection established")
        return dict(config.items(conn_nm))
    except Exception as error:
        logging.exception("get_config_section() is %s.", str(error))
        raise Exception("get_config_section(): " + str(error)) from error

def establish_conn(json_data: dict, json_section: str) -> bool:
    """establishes connection for the postgres database
       you pass it through the json"""
    try:
        connection_details = get_config_section(json_data["task"]["config_file"],\
        json_data["task"][json_section]["connection_name"])
        conn1 = sqlalchemy.create_engine(f'postgresql://{connection_details["user"]}'
        f':{connection_details["password"].replace("@", "%40")}@{connection_details["host"]}'
        f':{int(connection_details["port"])}/{connection_details["database"]}', encoding='utf-8')
        # logging.info("connection established")
        return conn1
    except Exception as error:
        logging.exception("establish_conn() is %s", str(error))
        raise Exception("establish_conn(): " + str(error)) from error

def db_table_exists(conn: dict, schema: str, tablename: str)-> bool:
    """ function for checking whether a table exists or not in postgres """
    # checking whether the table exists in database or not
    sql = f"select table_name from information_schema.tables where table_name='{tablename}'"\
          f"and table_schema='{schema}'"
    # return results of sql query from conn as a pandas dataframe
    results_df = pd.read_sql_query(sql, conn)
    # returns True if table exists else False
    return bool(len(results_df))


