""" script for reading data from xml"""
import logging
import pandas as pd

def read(json_data : dict) -> bool:
    """ function for reading data from json  """
    try:
        logging.info("json  reading started")
        logging.info("list of files which were read")
        datafram = pd.read_json(json_data["task"]["source"]["source_file_path"]+\
            json_data["task"]["source"]["source_file_name"],
        orient ='index',encoding = "utf-8", nrows = None)
        # df=pd.DataFrame(index=file.index).reset_index().astype(str)
        # frames = [df, file]
        # datafram = pd.concat(frames)
        datafram.columns = datafram.columns.astype(str)
        # print(file)
        yield datafram
        return True
    except Exception as error:
        logging.exception("reading json() is %s", str(error))
        raise Exception(" reading json(): " + str(error)) from error
