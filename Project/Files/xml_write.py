""" script for converting data to xml"""
import logging

def write(json_data: dict, chunk) -> bool:
    """ function for converting csv  to xml"""
    try:
        logging.info("converting data to xml initiated")
        chunk.to_xml(json_data["task"]["target"]["target_file_path"] + \
        json_data["task"]["target"]["target_file_name"],
                index=True)
        logging.info("csv to xml conversion completed")
        return True
    except Exception as error:
        logging.exception("convert_csv_to_xml() is %s", str(error))
        raise Exception("convert_csv_to_xml(): " + str(error)) from error
