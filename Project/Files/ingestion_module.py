""" importing modules """
import json
import definitions_4


# calling the logger function
definitions_4.initiate_logging('job_name_id', r"D:\\ingestion kart\\logging\\")
definitions_4.logging.info('logging initiated')

# reading the json file
try:
    with open(r"D:\\30 day python\\create code\\12345.json",'r', encoding='utf-8') as jsonfile:
        definitions_4.logging.info("reading json data")
        json_data = json.load(jsonfile)
        definitions_4.logging.info("reading json data completed")
except Exception as error:
    definitions_4.logging.exception("error in reading json %s.", str(error))
    raise Exception("error in reading json: " + str(error)) from error

# main script execution starts here
if json_data["task"]["task_type"]=="ingestion":
    definitions_4.logging.info("entered  in to ingestion")
    if json_data["task"]["source"]["source_type"] == "csvfile" and json_data["task"]["target"]["target_type"] == "postgres" :
        definitions_4.ingest_csv_data_to_postgres(json_data)
    elif json_data["task"]["source"]["source_type"]=="postgres" and json_data["task"]["target"]["target_type"]=="postgres":
        definitions_4.ingest_postgres_data_to_postgres(json_data)
    else:
        definitions_4.logging.warning("input proper source and target")
else:
    definitions_4.logging.info("only ingestion available currently")
