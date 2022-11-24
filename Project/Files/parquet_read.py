""" script for reading data from excel"""
import logging
import pandas as pd

def read(json_data : dict) -> bool:
    """ function for readinging data from parquet  """
    try:
        logging.info("parquet reading started")
        # Reading the data from Parquet File
        datafram = pd.read_parquet(json_data["task"]["source"]["source_file_path"]+\
            json_data["task"]["source"]["source_file_name"],
              engine='auto')
        yield datafram
        return True
    except Exception as error:
        logging.exception("reading_parquet_() is %s", str(error))
        raise Exception("rading_parquet(): " + str(error)) from error
