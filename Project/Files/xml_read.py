""" script for reading data from xml"""
import logging
import pandas as pd

def read(json_data : dict) -> bool:
    """ function for readinging data from xml  """
    try:
        logging.info("xml reading started")
        datafram = pd.read_xml(json_data["task"]["source"]["source_file_path"]+\
        json_data["task"]["source"]["source_file_name"],xpath='./*',parser='lxml')
        yield datafram
        return True
    except Exception as error:
        logging.exception("reading_xml() is %s", str(error))
        raise Exception("reading_xml(): " + str(error)) from error
        