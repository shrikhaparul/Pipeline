""" script for reading data from csv"""
import sys
import glob
import logging
import pandas as pd

def read(json_data: dict, delimiter = ",",
                           skip_header = 0, skip_footer= 0, quotechar = '"', escapechar = None
                           ) -> bool:
    """ function for reading data from csv"""
    try:
        logging.info("reading csv initiated...")
        path = json_data["task"]["source"]["source_file_path"]+\
        json_data["task"]["source"]["source_file_name"]
        # function for reading files present in a folder with different csv formats
        all_files = [f for f_ in [glob.glob(e) for e in (f'{path}*.zip',f'{path}*.csv',
        f'{path}*.csv.zip', f'{path}*.gz', f'{path}*.bz2') ] for f in f_]
        logging.info("list of files which were read")
        logging.info(all_files)
        if all_files == []:
            logging.error("'%s' SOURCE FILE not found in the location",json_data["task"]["source"]["source_file_name"])
            sys.exit()
        else:
            default_delimiter = delimiter if json_data["task"]["source"]["delimiter"]==" " else\
            json_data["task"]["source"]["delimiter"]
            default_skip_header = skip_header if json_data["task"]["source"]["skip_header"]==" " else\
            json_data["task"]["source"]["skip_header"]
            default_skip_footer = skip_footer if json_data["task"]["source"]["skip_footer"]==" " else\
            json_data["task"]["source"]["skip_footer"]
            default_quotechar = quotechar if json_data["task"]["source"]["quote_char"]==" " else\
            json_data["task"]["source"]["quote_char"]
            default_escapechar = escapechar if json_data["task"]["source"]["escape_char"]==" " else\
            json_data["task"]["source"]["escape_char"]
            default_select_cols = None if json_data["task"]["source"]["select_columns"]==" " else\
            list(json_data["task"]["source"]["select_columns"].split(","))
            default_alias_cols = None if json_data["task"]["source"]["alias_columns"]==" " else\
            list(json_data["task"]["source"]["alias_columns"].split(","))
            default_encoding = "utf-8" if json_data["task"]["source"]["encoding"]==" " else\
            json_data["task"]["source"]["encoding"]
            # print(default_alias_cols)
            default_header ='infer' if json_data["task"]["source"]["alias_columns"] == " " else None
            count = 0
            # df = pd.DataFrame()
            for file in all_files:
                count +=1
                data = pd.read_csv(filepath_or_buffer = file,encoding=default_encoding,low_memory=False)
                # print(type(data))
                row_count = data.shape[0]-default_skip_header-default_skip_footer
                count1 = 0
                # print(row_count)
                for chunk in pd.read_csv(filepath_or_buffer = file, names = default_alias_cols,
                header = default_header,sep = default_delimiter, usecols = default_select_cols,
                skiprows = default_skip_header,nrows = row_count,
                chunksize = json_data["task"]["source"]["chunk_size"],
                quotechar = default_quotechar, escapechar = default_escapechar,
                encoding = default_encoding):
                    count1 = 1 + count1
                    logging.info('%s iteration' , str(count1))
                    yield chunk
    except Exception as error:
        logging.exception("reading_csv() is %s", str(error))
        raise Exception("reading_csv(): " + str(error)) from error
        