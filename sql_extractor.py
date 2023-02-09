import pandas as pd
import pandas.io.sql as psql
import connect.sql_connect as sql
import sqlalchemy
import urllib
import logging

class SqlExtractor:
    def __init__(self, sql_connect, query, file_name, export_folder):
        self.sql_connect = sql_connect
        self.query = query
        self.export_folder = export_folder
        self.file_name = file_name

    def get_engine_string(self):
        """
        This method parses the given connection string to add double quotes
        :return: sqlalchemy engine connection string
        """
        conn_string = sql.SqlConnect.create_conn_string(self.sql_connect)
        logging.debug(conn_string)
        engine_string = 'mssql+pyodbc:///?odbc_connect={}'.format(urllib.parse.quote_plus(conn_string))
        logging.debug(engine_string)
        return engine_string

    def get_sql_engine(self):
        return sqlalchemy.create_engine(self.get_engine_string(), pool_size=5)

    def retrieve_data_psql(self, column_list):
        """
        Extract data based on given SQL connection and query pair using psql
        :return: results dataframe
        """
        try:
            conn = self.get_sql_engine()
            sql_query = psql.read_sql_query(sql=self.query, con=conn)
            df_results = pd.DataFrame(sql_query, columns=column_list)
            return df_results
        except Exception as e:
            print("Exception at retrieve_data_psql: " + str(e))
            logging.info("Exception at retrieve_data_psql: " + str(e))
            return None

    def get_parquet_file_name(self):
        """
        Add file extension for future file
        :return: string
        """
        return "{0}{1}.parquet".format(self.export_folder, self.file_name)

    def get_csv_file_name(self):
        """
        Add file extension for future file
        :return: string
        """
        return "{0}{1}.csv.gz".format(self.export_folder, self.file_name)

    def get_csv_file_name_split(self, chunk_no):
        """
        Add file extension for future file
        :return: string
        """
        return "{0}{1}_{2}.csv.gz".format(self.export_folder, self.file_name, str(chunk_no))

    def extract_single_record(self):
        with sql.SqlConnect.get_connected(self.sql_connect) as conn:
            cursor = conn.cursor()
            cursor.execute(self.query)
            return cursor.fetchone()

    def write_to_parquet(self, column_list, column_index='calendarDateCET'):
        """
        Convert sql results to DataFrame and write them to Parquet with Gzip compression
        :return: bool
        """
        output = False
        try:
            df_results = self.retrieve_data_psql(column_list)
            df_results.to_parquet(path=self.get_parquet_file_name(), compression='gzip',
                                  engine='auto', index=column_index, partition_cols=None)
            output = True
        except Exception as e:
            print("Exception at write_to_parquet: {0}".format(str(e)))
            logging.info("Exception at write_to_parquet: {0}".format(str(e)))
        return output

    def write_to_csv(self, column_list, delimiter):
        output = False
        try:
            df_rs = self.retrieve_data_psql(column_list)
            df_rs_clean = df_rs.replace('\n','', regex=True)
            df_results = df_rs_clean.replace('\|','',regex=True)
            if df_results is not None and df_results.shape[0] > 1:
                df_results.to_csv(self.get_csv_file_name(), header=False, index=False, sep=delimiter, compression='gzip')
                output = True
        except Exception as e:
            print("Exception at write_to_csv: {0}".format(str(e)))
            logging.info("Exception at write_to_csv: {0}".format(str(e)))
        return output

    def write_to_csv_split(self, column_list, delimiter, chunk_size=10**5):
        file_list = []
        try:
            conn = self.get_sql_engine()
            chunk_count = 0
            for chunk in psql.read_sql_query(sql=self.query, con=conn, chunksize=chunk_size):
                df_results = pd.DataFrame(chunk, columns=column_list)
                if df_results is not None and df_results.shape[0] > 1:
                    file_name = self.get_csv_file_name_split(chunk_count)
                    df_results.to_csv(file_name, header=False, index=False,
                                      sep=delimiter,
                                      compression='gzip')
                    file_list.append(file_name)
                    chunk_count += 1
        except Exception as e:
            print("Exception at write_to_csv: {0}".format(str(e)))
            logging.info("Exception at write_to_csv - chunksize: {0}".format(str(e)))
        return file_list
