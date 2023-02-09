import os
from datetime import datetime as dt
from datetime import timedelta
import connect.sql_connect as sql
import connect.s3_connect as s3
from extractor import sql_extractor as se
from utils.ConfigLoader import ConfigLoader
import sys
import logging

SQL_DRIVER_NAME = "ODBC Driver 17 for SQL Server"
_SUCCESS_FILE_NAME = "_SUCCESS"
_SUCCESS_FILE_CONTENT = """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut 
labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea 
commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla 
pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit 
anim id est laborum."""


class DwhExtractProcessor:
    def __init__(self, miner_start_date, miner_end_date, s3_bucket_name,
                 sql_database_name, sql_server_name, sql_user_name, sql_user_pwd):
        """
        It all starts here
        """
        self.start_date = miner_start_date
        self.end_date = miner_end_date
        self.s3_bucket_name = s3_bucket_name
        self.conf = ConfigLoader()
        self.info = print
        self.info("HOME_PATH:" + self.conf.HOME_PATH)
        self.FILE_EXPORT_PATH = self.conf.HOME_PATH
        self.success_file = self.create_s3_success_file()
        self.info("sql_user_name: {}".format(sql_user_name))
        self.info("sql_database_name: {}".format(sql_database_name))
        self.info("sql_server_name: {}".format(sql_server_name))
        self.sql_connect = sql.SqlConnect(sql_server_name, sql_database_name, sql_user_name, sql_user_pwd,
                                          SQL_DRIVER_NAME)

    def get_sql_connected(self):
        conn = sql.SqlConnect.get_connected(self.sql_connect)
        return conn

    def test_connection(self):
        output = sql.SqlConnect.test_connection(self.sql_connect)
        return output

    def create_s3_success_file(self):
        file_path = "{0}{1}".format(self.FILE_EXPORT_PATH, _SUCCESS_FILE_NAME)
        with open(file_path, 'w') as file:
            file.write(_SUCCESS_FILE_CONTENT)
        return file_path

    def get_export_file_name(self, prefix, date, hour):
        file_name = "{0}_{1}_{2}".format(prefix, date, hour)
        file_path = "calendar_date={1}/hour={2}".format(prefix, date, hour)
        file_path_name = "calendar_date={1}/hour={2}/{0}_{1}_{2}.csv".format(prefix, date, hour)
        return file_path, file_name

    def get_tdw_completed_query(self, package_name='tdwtdw_loadAll'):
        if self.conf.PACKAGE_NAME is not None:
            package_name = self.conf.PACKAGE_NAME
            print("Loading package_name from env config: {}".format(package_name))
        else:
            print("Using default package_name: {}".format(package_name))

        return """SELECT MAX(packageRunID) as packageRunID
            FROM processRepository.dbo.tpackageRunLog (NOLOCK)
            WHERE packageName = '{0}'
                  AND runStatus = 'OK'
                  AND startTime >= CONVERT(DATE, GETUTCDATE())
                  AND endTime IS NOT NULL;""".format(package_name)

    def is_tdw_ready(self):
        ready = False
        check_query = self.get_tdw_completed_query()
        print("Check query: {}".format(check_query))
        print("File export path: {}".format(self.FILE_EXPORT_PATH))
        check_extractor = se.SqlExtractor(self.sql_connect, check_query, "", self.conf.HOME_PATH)
        package_run_id = se.SqlExtractor.extract_single_record(check_extractor)
        if package_run_id is not None:
            ready = True
            with open("execute_check", "w+") as f:
                f.write("Execution start date {0} based on packageRunId {1}\n".format(self.start_date,
                                                                                      str(package_run_id[0])))
                f.close()
            print("TDW is ready!")
        return ready

    def extract(self):
        # Check tdw is ready
        print("Checking is TDW load is done")
        print("DISABLE_TDW_READY_CHECK: {}".format(self.conf.DISABLE_TDW_READY_CHECK))
        if self.conf.DISABLE_TDW_READY_CHECK is not None and self.conf.DISABLE_TDW_READY_CHECK == "FALSE":
            print("Calling is_tdw_ready()")
            tdw_status = self.is_tdw_ready()
        else:
            tdw_status = True

        print("TDW Load status: {}".format(tdw_status))
        if tdw_status:
            print("Extract process started")
            response = self.process_queries()
            print("Process Queries status: {}".format(response))
            if response == 200:
                print("Process Queries. Exit(0)")
                sys.exit(0)
            else:
                print("Process Queries. Exit(1)")
                sys.exit(1)
        else:
            print("TDW is not ready.")

    def process_queries(self):
        try:
            if self.conf.QUERIES is not None:
                queries = self.conf.QUERIES["extract"]
                for qry_info in queries:
                    logging.debug("Query: {}".format(qry_info))
                    type, output_path, query, columns, bucket_prefix, file_name_suffix, frequency = \
                        self.get_query_details(qry_info)
                    print(
                        "Type: {}. Output_Path: {}. Query: {}. Columns: {}. Bucket_Prefix: "
                        "{}. file_suffix: {}".format(type, output_path, query,columns, bucket_prefix, file_name_suffix
                                                     , frequency))
                    if frequency == "HOURLY":
                        self.get_extract_hourly(self.start_date, self.end_date, query, output_path, file_name_suffix
                                                , columns, bucket_prefix, frequency)
                        response = 200
                    elif frequency == "DAILY":
                        self.get_extract_daily(self.start_date, self.end_date, query, output_path, file_name_suffix
                                               , columns, bucket_prefix, frequency)
                        response = 200
                    elif frequency == "FULL":
                        self.get_extract_full(self.start_date, self.end_date, query, output_path, file_name_suffix
                                              , columns, bucket_prefix, frequency)
                        response = 200
                    else:
                        raise Exception("Unknown frequency type")
                return response
            else:
                print("Missing input configuration. Please check config")
                return 500
        except Exception as excp:
            print("Exception in process queries: {}".format(excp))
            return 500

    def get_extract_hourly(self, from_date, to_date, query, output_path, file_name_suffix, columns, bucket_prefix
                           , frequency):
        print("from_date: {}. to_date: {}".format(from_date, to_date))
        iter_date = from_date
        while iter_date <= to_date:
            fmt_dt = iter_date.strftime('%Y-%m-%d')
            for hour in range(0, 24):
                print("Extracting for date: " + fmt_dt + ". Hour: " + str(hour))
                fmt_qry = query.format(iter_date, hour)
                print("Extract Query: {}".format(fmt_qry))
                file_path, file_name = self.get_export_file_name(file_name_suffix, fmt_dt, hour)
                print("file_path: {}".format(file_path))
                print("file_name: {}".format(file_name))
                qry_extractor = se.SqlExtractor(self.sql_connect, fmt_qry, file_name, self.conf.HOME_PATH)
                extracted = se.SqlExtractor.write_to_csv(qry_extractor, columns, delimiter='|')
                if extracted:
                    csv_file = se.SqlExtractor.get_csv_file_name(qry_extractor)
                    print("csv_file: {}".format(csv_file))
                    print("bucket name: {}".format(self.s3_bucket_name))
                    object_key = self.get_s3_object_key(bucket_prefix, fmt_dt, hour, csv_file, self.conf.HOME_PATH)
                    logging.debug('object key: {}'.format(object_key))
                    s3_connect = s3.S3Connect(csv_file,
                                              self.s3_bucket_name,
                                              object_key, run_env=self.conf.RUN_ENV)
                    uploaded = s3.S3Connect.upload_file(s3_connect, self.conf.RUN_ENV)
                    # uploaded = False
                    if uploaded:
                        self.mark_s3_upload_success(csv_file, iter_date, object_key)
                        print("Completed extract and upload of file {0}. We will remove it".format(csv_file))
            iter_date = iter_date + timedelta(days=1)
            print("Next date: {}".format(iter_date))

    def get_extract_daily(self, from_date, to_date, query, output_path, file_name_suffix, columns, bucket_prefix, frequency):
        print("from_date: {}. to_date: {}".format(from_date, to_date))
        fmt_dt = from_date.strftime('%Y-%m-%d')
        # Replace start and end date
        fmt_qry = query.format(start_date, end_date)
        file_path, file_name = self.get_export_file_name(file_name_suffix, fmt_dt, 0)
        print("Extract Query: {}".format(fmt_qry))
        print("file_path: {}".format(file_path))
        print("file_name: {}".format(file_name))
        qry_extractor = se.SqlExtractor(self.sql_connect, fmt_qry, file_name, self.conf.HOME_PATH)
        extracted = se.SqlExtractor.write_to_csv(qry_extractor, columns, delimiter='|')
        if extracted:
            csv_file = se.SqlExtractor.get_csv_file_name(qry_extractor)
            print("output csv file name: {}".format(csv_file))
            object_key = self.get_s3_object_key(bucket_prefix, fmt_dt, 0, csv_file, self.conf.HOME_PATH)
            print('object key: {}'.format(object_key))
            s3_connect = s3.S3Connect(csv_file,
                                      self.s3_bucket_name,
                                      object_key, run_env=self.conf.RUN_ENV)
            uploaded = s3.S3Connect.upload_file(s3_connect, self.conf.RUN_ENV)
            # uploaded = False
            if uploaded:
                self.mark_s3_upload_success(csv_file, fmt_dt, object_key)
                print("Completed extract and upload of file {0}. We will remove it".format(csv_file))

    def get_extract_full(self, from_date, to_date, query, output_path, file_name_suffix, columns, bucket_prefix, frequency):
        fmt_dt = from_date.strftime('%Y-%m-%d')
        file_path, file_name = self.get_export_file_name(file_name_suffix, fmt_dt, 0)
        print("Frequency: {}".format(query))
        print("Extract Query: {}".format(query))
        print("file_path: {}".format(file_path))
        print("file_name: {}".format(file_name))
        qry_extractor = se.SqlExtractor(self.sql_connect, query, file_name, self.conf.HOME_PATH)
        extracted = se.SqlExtractor.write_to_csv(qry_extractor, columns, delimiter='|')
        if extracted:
            csv_file = se.SqlExtractor.get_csv_file_name(qry_extractor)
            logging.debug("csv_file: {}".format(csv_file))
            logging.debug("bucket name: {}".format(self.s3_bucket_name))
            object_key = self.get_s3_object_key(bucket_prefix, fmt_dt, 0, csv_file, self.conf.HOME_PATH)
            logging.debug('object key: {}'.format(object_key))
            s3_connect = s3.S3Connect(csv_file,
                                      self.s3_bucket_name,
                                      object_key, run_env=self.conf.RUN_ENV)
            uploaded = s3.S3Connect.upload_file(s3_connect, self.conf.RUN_ENV)
            # uploaded = False
            if uploaded:
                self.mark_s3_upload_success(csv_file, fmt_dt, object_key)
                print("Completed extract and upload of file {0}. We will remove it".format(csv_file))

    @staticmethod
    def get_query_details(d_qry):
        logging.debug("get_query_details(): {}".format(d_qry))
        type = d_qry["type"]
        output_path = d_qry["out_path"]
        query = d_qry["query"]
        columns = d_qry["columns"]
        bucket_prefix = d_qry["bucket_prefix"]
        file_name_suffix = d_qry["file_name_suffix"]
        frequency = d_qry["frequency"].upper()
        return type, output_path, query, columns, bucket_prefix, file_name_suffix, frequency

    @staticmethod
    def get_s3_object_key(bucket_prefix, extract_date, hour, file_name, export_path):
        return "{0}/calendardate={1}/hour={2}/{3}".format(bucket_prefix, extract_date, hour,
                                                          file_name.replace(export_path, ''))

    def mark_s3_upload_success(self, file_name, file_date, object_key):
        s3_clean_path = object_key.replace(file_name.replace(self.conf.HOME_PATH, ''), '')
        logging.debug("s3_clean_path: {}".format(s3_clean_path))
        s3_success_path = "{0}{1}".format(s3_clean_path, _SUCCESS_FILE_NAME)
        logging.debug("s3_success_path: {}".format(s3_success_path))
        s3_connect = s3.S3Connect(self.success_file, self.s3_bucket_name, s3_success_path, self.conf.RUN_ENV)
        s3_folder_count = s3.S3Connect.count_files_in_folder(s3_connect, s3_clean_path, self.conf.RUN_ENV)
        if s3_folder_count >= 23:
            s3.S3Connect.upload_file(s3_connect, self.conf.RUN_ENV)
        if s3_folder_count >= 23:
            s3.S3Connect.upload_file(s3_connect, self.conf.RUN_ENV)

    @staticmethod
    def delete_finished_day_files(self, files_date):
        files = os.listdir(self.FILE_EXPORT_PATH)
        for f in files:
            if str(files_date)[0:10] in f:
                os.remove("{0}{1}".format(self.FILE_EXPORT_PATH, f))
        print("Removed files for: {0}".format(str(files_date)[0:10]))


def parse_arguments():
    args_list = sys.argv
    if len(args_list) > 1:
        return args_list[1:]
    else:
        return None


def get_start_date(date_args):
    date_0 = dt.strptime(date_args[0], '%Y-%m-%d')
    date_1 = dt.strptime(date_args[1], '%Y-%m-%d')
    if date_0 < date_1:
        return date_0
    else:
        return date_1


def get_end_date(date_args):
    date_0 = dt.strptime(date_args[0], '%Y-%m-%d')
    date_1 = dt.strptime(date_args[1], '%Y-%m-%d')
    if date_0 > date_1:
        return date_0
    else:
        return date_1


if __name__ == "__main__":
    """
    Send the miner to do his job
    """
    received_args = parse_arguments()
    if received_args is not None:
        start_date = get_start_date(received_args)
        end_date = get_end_date(received_args)
        s3_bucket_name = received_args[2]
        sql_database_name = received_args[3]
        sql_server_name = received_args[4]
        sql_user_name = received_args[5]
        sql_user_pwd = received_args[6]
        dwh_miner = DwhExtractProcessor(start_date, end_date, s3_bucket_name,
                                        sql_database_name, sql_server_name, sql_user_name, sql_user_pwd)
        dwh_miner.extract()
        # miner = DwhMiner(start_date, end_date, s3_bucket_name, sql_database_name,sql_server_name, sql_user_name, sql_user_pwd)
        # DwhMiner.start_the_execution(miner)
    else:
        print("Missing expected params: e.g. python dwh_processor.py 2020-04-24 2020-04-17")
        miner_start_date = get_start_date(['2022-05-02', '2022-05-06'])
        miner_end_date = get_end_date(['2022-05-02', '2022-05-03'])
        s3_bucket_name = 'datasson-lake-dev'
        sql_database_name = 'TDW'
        sql_server_name = 'STO-BI-DEV01.bde.local'
                          #'bma-sql13-mig02.ble.local'
        sql_user_name = f'BDE\\roch01'
        sql_user_pwd = os.getenv("BLE_PASSWORD")
        dwh_miner = DwhExtractProcessor(miner_start_date, miner_end_date, s3_bucket_name, sql_database_name
                                        , sql_server_name, sql_user_name, sql_user_pwd)
        print("Get Connection Object")
        dwh_miner.get_sql_connected()
        print("Check connection")
        dwh_miner.test_connection()
        print("Check if tdw is ready")
        dwh_miner.extract()
        '''
        print("Missing expected params: e.g. python dwh_processor.py 2020-04-24 2020-04-17")
        miner_start_date = get_start_date(['2022-05-02', '2022-05-06'])
        miner_end_date = get_end_date(['2022-05-02', '2022-05-03'])
        s3_bucket_name = 'datasson-lake-dev'
        sql_database_name = 'TDW'
        sql_server_name = 'bma-sql13-mig02.ble.local'
        sql_user_name = f'BLE\\roch01'
        sql_user_pwd = os.getenv("BLE_PASSWORD")
        dwh_miner = DwhExtractProcessor(miner_start_date, miner_end_date, s3_bucket_name, sql_database_name
                                        , sql_server_name, sql_user_name, sql_user_pwd)
        print("Get Connection Object")
        dwh_miner.get_sql_connected()
        print("Check connection")
        dwh_miner.test_connection()
        print("Check if tdw is ready")
        dwh_miner.extract()
        '''