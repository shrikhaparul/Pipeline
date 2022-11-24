""" script for writing data to csv file"""
import logging

def write(json_data: dict,datafram, counter) -> bool:
    """ function for writing data to csv file"""
    try:
        logging.info("writing data to csv file")
        if counter ==1:
            datafram.to_csv(json_data["task"]["target"]["target_file_path"],
            sep=json_data["task"]["target"]["file_delimiter"], header=True,
            index=False, mode='a', encoding=json_data["task"]["target"]["encoding"])
        else:
            datafram.to_csv(json_data["task"]["target"]["target_file_path"],
            sep=json_data["task"]["target"]["file_delimiter"], header=False, index=False,
            mode='a', encoding=json_data["task"]["target"]["encoding"])
        return True
    except Exception as error:
        logging.exception("ingest_data_to_csv() is %s", str(error))
        raise Exception("ingest_data_to_csv(): " + str(error)) from error
