""" script for writing data to postgres table"""
import sys
import logging
import sqlalchemy
from utility import get_config_section

def db_table_exists(conn: dict, schema: str, tablename: str)-> bool:
    """ function for checking whether a table exists or not in postgres """
    # checking whether the table exists in database or not
    sql = f"select table_name from information_schema.tables where table_name='{tablename}'"\
          f"and table_schema='{schema}'"
    connection = conn.raw_connection()
    cursor = connection.cursor()
    cursor.execute(sql)
    # print(bool(cursor.rowcount))
    return bool(cursor.rowcount)
    # return results of sql query from conn as a pandas dataframe
    # results_df = pd.read_sql_query(sql, conn)
    # returns True if table exists else False
    # return bool(len(results_df))

def establish_conn(json_data: dict, json_section: str) -> bool:
    """establishes connection for the postgres database
       you pass it through the json"""
    try:
        connection_details = get_config_section(json_data["task"][json_section]["connection_file_path"],\
        json_data["task"][json_section]["connection_name"])
        conn1 = sqlalchemy.create_engine(f'postgresql://{connection_details["user"]}'
        f':{connection_details["password"].replace("@", "%40")}@{connection_details["host"]}'
        f':{int(connection_details["port"])}/{connection_details["database"]}')
        # logging.info("connection established")
        return conn1
    except Exception as error:
        logging.exception("establish_conn() is %s", str(error))
        raise Exception("establish_conn(): " + str(error)) from error

def create(json_data: dict, conn, dataframe) -> bool:
    """if table is not present , it will create"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"],\
        json_data["task"]["target"]["table_name"]) is False:
            logging.info('%s does not exists so creating a new table',\
            json_data["task"]["target"]["table_name"])
            dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,\
             schema = json_data["task"]["target"]["schema"],index = False, if_exists = "append")
            logging.info("postgres ingestion completed")
        else:
            # if table exists, it will say table is already present, give new name to create
            logging.error('%s already exists, so give a new table name to create',
            json_data["task"]["target"]["table_name"])
            sys.exit()
    except Exception as error:
        logging.exception("create() is %s", str(error))
        raise Exception("create(): " + str(error)) from error

def append(json_data: dict, conn: dict, dataframe) -> bool:
    """if table exists, it will append"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"],
        json_data["task"]["target"]["table_name"]) is True:
            logging.info("%s table exists, started appending the data to table",
            json_data["task"]["target"]["table_name"])
            dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
            schema = json_data["task"]["target"]["schema"],index = False, if_exists = "append")
            logging.info("postgres ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            # create table first or give table name that exists to append data
            logging.error('%s does not exists, so create table first',\
            json_data["task"]["target"]["table_name"])
            sys.exit()
    except Exception as error:
        logging.exception("append() is %s", str(error))
        raise Exception("append(): " + str(error)) from error

def truncate(json_data: dict, conn: dict) -> bool:
    """if table exists, it will truncate"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"],\
            json_data["task"]["target"]["table_name"]) is True:
            logging.info("%s table exists, started truncating the table",
            json_data["task"]["target"]["table_name"])
            truncate_query = sqlalchemy.text(f'TRUNCATE TABLE'
            f'{json_data["task"]["target"]["schema"]}.'
            f'{json_data["task"]["target"]["table_name"]}')
            conn.execution_options(autocommit=True).execute(truncate_query)
            logging.info("postgres truncating table completed")
            sys.exit()
            # logging.info("truncating table finished, started inserting data into
            #  %s table", json_data["table"])
            # for chunk in dataframe:
            #     chunk.to_sql(json_data["table"], conn, schema = json_data["schema"],
            #     index = False, if_exists = "append"
            # )
            # logging.info("postgres ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            logging.error('%s does not exists, give correct table name to truncate',
            json_data["task"]["target"]["table_name"])
            sys.exit()
    except Exception as error:
        logging.exception("truncate() is %s", str(error))
        raise Exception("truncate(): " + str(error)) from error

def drop(json_data: dict, conn: dict) -> bool:
    """if table exists, it will drop"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"],
        json_data["task"]["target"]["table_name"]) is True:
            logging.info("%s table exists, started dropping the table",
            json_data["task"]["target"]["table_name"])
            drop_query = sqlalchemy.text(f'DROP TABLE {json_data["task"]["target"]["schema"]}.'
            f'{json_data["task"]["target"]["table_name"]}')
            conn.execution_options(autocommit=True).execute(drop_query)
            logging.info("postgres dropping table completed")
            sys.exit()
            # logging.info(" table drop finished, started inserting data into
            #  %s table", json_data["table"])
            # for chunk in dataframe:
            #     chunk.to_sql(json_data["table"], conn, schema = json_data["schema"],
            #     index = False, if_exists = "append"
            # )
        else:
            # if table is not there, then it will say table does not exist
            logging.error('%s does not exists, give correct table name to drop',
            json_data["task"]["target"]["table_name"])
            sys.exit()
    except Exception as error:
        logging.exception("drop() is %s", str(error))
        raise Exception("drop(): " + str(error)) from error

def replace(json_data: dict, conn: dict, dataframe,counter: int) -> bool:
    """if table exists, it will drop and replace data"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["schema"],
        json_data["task"]["target"]["table_name"]) is True:
            if counter == 1:
                logging.info("%s table exists, started replacing the table",
                json_data["task"]["target"]["table_name"])
                replace_query = sqlalchemy.text(f'DROP TABLE'
                f'{json_data["task"]["target"]["schema"]}.'
                f'{json_data["task"]["target"]["table_name"]}')
                conn.execution_options(autocommit=True).execute(replace_query)
                logging.info(" table replace finished, started inserting data into "
                 "%s table", json_data["task"]["target"]["table_name"])
                dataframe.to_sql(json_data["task"]["target"]["table_name"],conn, schema =
                json_data["task"]["target"]["schema"],index = False, if_exists = "append")
                logging.info("postgres ingestion completed")
            else:
                dataframe.to_sql(json_data["task"]["target"]["table_name"], conn, schema =
                json_data["task"]["target"]["schema"],
                    index = False, if_exists = "append")
                logging.info("postgres ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            logging.error('%s does not exists, give correct table name',\
            json_data["task"]["target"]["table_name"])
            sys.exit()
    except Exception as error:
        logging.exception("replace() is %s", str(error))
        raise Exception("replace(): " + str(error)) from error

def write(json_data: dict, datafram, counter: int) -> bool:
    """ function for ingesting data to postgres based on the operation in json"""
    try:
        logging.info("ingest data to postgres db initiated")
        conn2 = establish_conn(json_data, 'target')
        if json_data["task"]["target"]["operation"] == "create":
            if counter == 1:
                create(json_data, conn2, datafram)
            else:
                append(json_data, conn2, datafram)
        elif json_data["task"]["target"]["operation"] == "append":
            append(json_data, conn2, datafram)
        elif json_data["task"]["target"]["operation"] == "truncate":
            truncate(json_data, conn2)
        elif json_data["task"]["target"]["operation"] == "drop":
            drop(json_data, conn2)
        elif json_data["task"]["target"]["operation"] == "replace":
            replace(json_data, conn2, datafram, counter)
        elif json_data["task"]["target"]["operation"] not in ("create", "append","truncate",
            "drop","replace"):
            logging.error("give proper input for operation to be performed on table")
            sys.exit()
        conn2.dispose()
        return True
    except Exception as error:
        logging.exception("ingest_data_to_postgres() is %s", str(error))
        raise Exception("ingest_data_to_postgres(): " + str(error)) from error
        