""" importing modules """
import configparser
import logging
import glob
import sqlalchemy
import pandas as pd

# custom log function for framework
def initiate_logging(job_id: str, log_loc: str) -> bool:
    """Function to initiate log file which will be used across framework"""
    try:
        log_file_nm = job_id + '.log'
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
        logging.info("connection established")
        return conn1
    except Exception as error:
        logging.exception("establish_conn() is %s", str(error))
        raise Exception("establish_conn(): " + str(error)) from error

def db_table_exists(conn, schema, tablename):
    """ function for checking whether a table exists or not in postgres """
    # checking whether the table exists in database or not
    sql = f"select table_name from information_schema.tables where table_name='{tablename}'"\
          f"and table_schema='{schema}'"
    # return results of sql query from conn as a pandas dataframe
    results_df = pd.read_sql_query(sql, conn)
    # returns True if table exists else False
    return bool(len(results_df))

# code to read csv and ingest into table in a postgres database
def reading_csv(json_data: dict, delimiter = ",",
                           header1 = 0, footer= 0, quotechar = '"', escapechar = None
                           ) -> bool:
    """ function for reading data from csv"""
    try:
        logging.info("reading csv initiated...")
        path = json_data["task"]["source"]["source_file_path"]+json_data["task"]["source"]["source_file_name"]
        # function for reading files present in a folder with different csv formats
        all_files = [f for f_ in [glob.glob(e) for e in (f'{path}*.csv',
        f'{path}*.csv.zip', f'{path}*.gz', f'{path}*.bz2') ] for f in f_]
        # x = json_data["csv_loc"] +'"'+ json_data["file_name"]+'"'
        logging.info("list of files which were read")
        logging.info(all_files)
        default_delimiter = delimiter if json_data["task"]["source"]["delimiter"]==" " else json_data["task"]["source"]["delimiter"]
        default_header = header1 if json_data["task"]["source"]["header"]==" " else json_data["task"]["source"]["header"]
        default_footer = footer if json_data["task"]["source"]["footer"]==" " else json_data["task"]["source"]["footer"]
        default_quotechar = quotechar if json_data["task"]["source"]["quote_char"]==" " else json_data["task"]["source"]["quote_char"]
        default_escapechar = escapechar if json_data["task"]["source"]["escape_char"]==" " else json_data["task"]["source"]["escape_char"]
        default_usecols = None if json_data["task"]["source"]["columns"]==" " else list(json_data["task"]["source"]["columns"].split(","))
        count = 0
        # df = pd.DataFrame()
        for file in all_files:
            count +=1
            data = pd. read_csv(filepath_or_buffer = file, encoding='latin1')
            # print(type(data))
            row_count = data.shape[0]-default_header-default_footer
            print(row_count)
            # print(data.columns.values.tolist())
            datafram = pd.read_csv(filepath_or_buffer = file,
            sep = default_delimiter, usecols = default_usecols, skiprows = default_header,
            nrows = row_count, chunksize = json_data["task"]["source"]["chunk_size"],
            quotechar = default_quotechar, escapechar = default_escapechar,
            encoding='latin1')
            # df.append(df2, ignore_index = True)
            logging.info(file)
            logging.info("reading csv completed")
        return datafram
    except Exception as error:
        logging.exception("reading_csv() is %s", str(error))
        raise Exception("reading_csv(): " + str(error)) from error

def create(json_data: dict, conn, dataframe) -> bool:
    """if table is not present , it will create table in postgres"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"], json_data["task"]["target"]["table_name"]) is False:
            logging.info('%s does not exists so creating a new table',json_data["task"]["target"]["table_name"])
            for chunk in dataframe:
                chunk.to_sql(json_data["task"]["target"]["table_name"], conn, schema = json_data["task"]["target"]["schema"],
                index = False, if_exists = "append")
            logging.info("postgres ingestion completed")
        else:
            # if table exists, it will say table is already present, give new name to create
            logging.error('%s already exists, so give a new table name to create',
            json_data["task"]["target"]["table_name"])
    except Exception as error:
        logging.exception("create() is %s", str(error))
        raise Exception("create(): " + str(error)) from error

def append(json_data: dict, conn, dataframe) -> bool:
    """if table exists, it will append data to the existing table"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"], json_data["task"]["target"]["table_name"]) is True:
            logging.info("%s table exists, started appending the data to table",
            json_data["task"]["target"]["table_name"])
            for chunk in dataframe:
                chunk.to_sql(json_data["task"]["target"]["table_name"], conn, schema = json_data["task"]["target"]["schema"],
                index = False, if_exists = "append")
            logging.info("postgres ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            # create table first or give table name that exists to append data
            logging.error('%s does not exists, so create table first',json_data["task"]["target"]["table_name"])
    except Exception as error:
        logging.exception("append() is %s", str(error))
        raise Exception("append(): " + str(error)) from error

def truncate(json_data: dict, conn) -> bool:
    """if table exists, it will truncate the data from the table"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"], json_data["task"]["target"]["table_name"]) is True:
            logging.info("%s table exists, started truncating the table",
            json_data["task"]["target"]["table_name"])
            truncate_query = sqlalchemy.text(f'TRUNCATE TABLE {json_data["task"]["target"]["schema"]}.'
            f'{json_data["task"]["target"]["table_name"]}')
            conn.execution_options(autocommit=True).execute(truncate_query)    
            logging.info("truncating table completed")
        else:
            # if table is not there, then it will say table does not exist
            logging.error('%s does not exists, give correct table name',json_data["task"]["target"]["table_name"])
    except Exception as error:
        logging.exception("append() is %s", str(error))
        raise Exception("append(): " + str(error)) from error

def drop(json_data: dict, conn) -> bool:
    """if table exists, it will drop the table"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"], json_data["task"]["target"]["table_name"]) is True:
            logging.info("%s table exists, started dropping the table", json_data["task"]["target"]["table_name"])
            drop_query = sqlalchemy.text(f'DROP TABLE {json_data["task"]["target"]["schema"]}.'
            f'{json_data["task"]["target"]["table_name"]}')
            conn.execution_options(autocommit=True).execute(drop_query)
            logging.info("dropping table completed")
        else:
            # if table is not there, then it will say table does not exist
            logging.error('%s does not exists, give correct table name',json_data["task"]["target"]["table_name"])
    except Exception as error:
        logging.exception("append() is %s", str(error))
        raise Exception("append(): " + str(error)) from error

def replace(json_data: dict, conn, dataframe) -> bool:
    """if table exists, it will drop and replace data"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"], json_data["task"]["target"]["table_name"]) is True:
            logging.info("%s table exists, started replacing the table", json_data["task"]["target"]["table_name"])
            replace_query = sqlalchemy.text(f'DROP TABLE {json_data["task"]["target"]["schema"]}.'
            f'{json_data["task"]["target"]["table_name"]}')
            conn.execution_options(autocommit=True).execute(replace_query)
            logging.info(" table replace finished, started inserting data into"
             "%s table", json_data["task"]["target"]["table_name"])
            for chunk in dataframe:
                chunk.to_sql(json_data["task"]["target"]["table_name"], conn, schema = json_data["task"]["target"]["schema"],
                index = False, if_exists = "append")
            logging.info("replacing  data completed")
        else:
            # if table is not there, then it will give error saying table does not exist
            logging.error('%s does not exists, give correct table name',json_data["task"]["target"]["table_name"])
    except Exception as error:
        logging.exception("append() is %s", str(error))
        raise Exception("append(): " + str(error)) from error

def ingest_csv_data_to_postgres(json_data: dict) -> bool:
    """ function for ingesting data to postgres based on the operation in json"""
    try:
        logging.info("ingest csv to postgres data initiated")
        conn2 = establish_conn(json_data, 'target')
        df1 = reading_csv(json_data)
        if json_data["task"]["target"]["if_exists"] == "create":
            create(json_data, conn2, df1)
        elif json_data["task"]["target"]["if_exists"] == "append":
            append(json_data, conn2, df1)
        elif json_data["task"]["target"]["if_exists"] == "truncate":
            truncate(json_data, conn2)
        elif json_data["task"]["target"]["if_exists"] == "drop":
            drop(json_data, conn2)
        elif json_data["task"]["target"]["if_exists"] == "replace":
            replace(json_data, conn2, df1)
        conn2.dispose()
        return True
    except Exception as error:
        logging.exception("ingest_data_to_postgres() is %s", str(error))
        raise Exception("ingest_data_to_postgres(): " + str(error)) from error

# code to ingest data from one table in postgres to other table in postgres
def read_data_from_postgres(json_data: dict) -> bool:
    """ function for reading data from postgres table"""
    try:
        logging.info("reading data from postgres table initiated")
        # creating source connection to postgres
        conn3 = establish_conn(json_data, 'source')
        logging.info('reading data from source started')
        default_columns = None if json_data["task"]["source"]["columns"]==" " else list(json_data["task"]["source"]["columns"]
        .split(","))
        query = pd.read_sql_table(
            json_data["task"]["source"]["table_name"], conn3, json_data["task"]["source"]["schema"],
            columns = default_columns, chunksize = json_data["task"]["source"]["chunk_size"])
        conn3.dispose()
        return query
    except Exception as error:
        logging.exception("postgres_to_postgres() is %s", str(error))
        raise Exception("postgres_to_postgres(): " + str(error)) from error


def ingest_postgres_data_to_postgres(json_data: dict) -> bool:
    """function for ingesting data to postgres table"""
    try:
        logging.info("ingesting data to postgres initiated")
        table_data = read_data_from_postgres(json_data)
        db2 = establish_conn(json_data, 'target')
        logging.info("established target postgres connection")
        logging.info("writing to target postgres database started")
        if json_data["task"]["target"]["if_exists"] == "create":
            create(json_data, db2, table_data)
        elif json_data["task"]["target"]["if_exists"] == "append":
            append(json_data, db2, table_data)
        elif json_data["task"]["target"]["if_exists"] == "truncate":
            truncate(json_data, db2)
        elif json_data["task"]["target"]["if_exists"] == "drop":
            drop(json_data, db2)
        elif json_data["task"]["target"]["if_exists"] == "replace":
            replace(json_data, db2, table_data)
        db2.dispose()
        return True
    except Exception as error:
        logging.exception("ingest_postgres_data_to_postgres() is %s", str(error))
        raise Exception("ingest_postgres_data_to_postgres(): " + str(error)) from error
