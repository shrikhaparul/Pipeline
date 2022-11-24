""" script for convrting data to json"""
import logging

def write(json_data: dict, datafram) -> bool:
    """ function for converting csv  to  json"""
    try:
        logging.info("converting data to json initiated")
        datafram.to_json(json_data["task"]["target"]["target_file_path"] + \
        json_data["task"]["target"]["target_file_name"],
                index=True)
        logging.info("csv to json conversion completed")
        return True
    except Exception as error:
        logging.exception("convert_csv_to_json() is %s", str(error))
        raise Exception("convert_csv_to_json(): " + str(error)) from error
