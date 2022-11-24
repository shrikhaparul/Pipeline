""" script for convrting data to excel"""
import logging

def write(json_data: dict, datafram) -> bool:
    """ function for converting csv  to  excel"""
    try:
        logging.info("converting data to excel initiated")
        datafram.to_excel(json_data["task"]["target"]["target_file_path"] + \
        json_data["task"]["target"]["target_file_name"],
                index=False)
        logging.info("csv to excel conversion completed")
        return True
    except Exception as error:
        logging.exception("convert_csv_to_excel() is %s", str(error))
        raise Exception("convert_csv_to_excel(): " + str(error)) from error
        