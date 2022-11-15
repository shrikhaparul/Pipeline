""" script for reading data from csv"""
import glob
import logging
import pandas as pd

def read(json_data: dict, delimiter = ",",
                           header1 = 0, footer= 0, quotechar = '"', escapechar = None
                           ) -> bool:
    """ function for reading data from csv"""
    try:
        logging.info("reading csv initiated...")
        path = json_data["task"]["source"]["source_file_path"]+json_data["task"]["source"]["source_file_name"]
        # function for reading files present in a folder with different csv formats
        all_files = [f for f_ in [glob.glob(e) for e in (f'{path}*.csv',
        f'{path}*.csv.zip', f'{path}*.gz', f'{path}*.bz2') ] for f in f_]
        # x = json_data["csv_loc"] +'"'+ json_data["file_name"]+'"'
        logging.info("list of files which were read")
        logging.info(all_files)
        # all_files = glob.glob("C:\\Users\\RaviKiranPerumandla\\Downloads\\
        # Combined_Flights\\sales_data.csv")
        # conn = sqlalchemy.create_engine(f'postgresql://{user}:{password}@
        # {host}:{port}/{data_base}', encoding='utf-8')
        # print(conn)
        default_delimiter = delimiter if json_data["task"]["source"]["delimiter"]==" " else json_data["task"]["source"]["delimiter"]
        default_header = header1 if json_data["task"]["source"]["header"]==" " else json_data["task"]["source"]["header"]
        default_footer = footer if json_data["task"]["source"]["footer"]==" " else json_data["task"]["source"]["footer"]
        default_quotechar = quotechar if json_data["task"]["source"]["quote_char"]==" " else json_data["task"]["source"]["quote_char"]
        default_escapechar = escapechar if json_data["task"]["source"]["escape_char"]==" " else json_data["task"]["source"]["escape_char"]
        default_usecols = None if json_data["task"]["source"]["columns"]==" " else list(json_data["task"]["source"]["columns"].split(","))
        count = 0
        # df = pd.DataFrame()
        for file in all_files:
            count +=1
            data = pd.read_csv(filepath_or_buffer = file, encoding='latin1')
            # print(type(data))
            row_count = data.shape[0]-default_header-default_footer
            # print(row_count)
            # print(row_count)encoding
            # print(data.columns.values.tolist())
            count1 = 0
            for chunk in pd.read_csv(filepath_or_buffer = file,
            sep = default_delimiter, usecols = default_usecols, skiprows = default_header,
            nrows = row_count, chunksize = json_data["task"]["source"]["chunk_size"],
            quotechar = default_quotechar, escapechar = default_escapechar,
            encoding='latin1'):
                count1 = 1 + count1
                logging.info('%s iteration' , str(count1))
                yield chunk
            # df.append(df2, ignore_index = True)
            # logging.info(file)
            # logging.info("reading csv completed")
            # return count1
        # return datafram
    except Exception as error:
        logging.exception("reading_csv() is %s", str(error))
        raise Exception("reading_csv(): " + str(error)) from error