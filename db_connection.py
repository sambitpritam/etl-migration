import logging
import pyodbc
import os

from dotenv import load_dotenv


class DB_Connection:

    def __init__(self):
        load_dotenv()
        self.log = logging.getLogger(__name__)
        self.uid = os.getenv("UID")
        self.pwd = os.getenv("PWD")
        self.database = os.getenv("DATABASE")
        self.server = os.getenv("SERVER")


    def get_db_connection(self, db_engine: str):
        conn = None
        try:
            if db_engine.lower() == "mssql":
                driver = "{SQL Server}"
            else:
                raise Exception(f"Un-supported DB_ENGINE value: {db_engine}. This script only supports values: 'mssql'")
            
            self.log.info("Creating MSSQL DB Connection...")
            conn = pyodbc.connect(
                f'DRIVER={driver};'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'UID={self.uid};'
                f'PWD={self.pwd};'
            )

        except Exception as e:
            self.log.error(f"Exception Occurred: {e}")

        return conn

    def execute_fetch_query(self, query: str, db_engine: str):
        results = None
        conn = self.get_db_connection(db_engine=db_engine)

        if conn is not None:
            self.log.info("MSSQL DB Connection created successfully...")
            cur = conn.cursor()

            self.log.info(f"Executing query: {query}")
            cur.execute(query)

            results = cur.fetchall()

        else:
            self.log.error("ERROR: Unable to create MSSQL DB Connection.")

        return results


    def download_file(bucket, object_name, dest_path):
        """download a file to an S3 bucket

        :param bucket: Bucket to upload to
        :param object_name: S3 object name including file_name
        :return: True if file was downloaded, else False
        """

        import boto3
        from botocore.exceptions import ClientError

        file_name = object_name.split("/")[-1]

        # If dest_path does not exists
        if not os.path.exists(dest_path):
            os.makedirs(dest_path)

        # download the file
        s3_client = boto3.client('s3', aws_access_key_id="**********", 
                        aws_secret_access_key="*******")
        try:
            response = s3_client.download_file(bucket, object_name, os.path.join(dest_path, file_name))
        except ClientError as e:
            logging.error(e)
            return False
        return True


    def convert_json_to_csv(self, filename: None):
        """convert json to csv file

        :param filename: (optional) json file name to be converted to csv 
        """

        import pandas as pd

        if filename is not None or not os.path.exists(filename):
            src_path = "C:\\tmp\\json-2-csv\\input.json"
        else:
            src_path = filename
        
        dest_path = "C:\\tmp\\json-2-csv\\"

        if not os.path.exists(dest_path):
            os.makedirs(dest_path)

        try:

            with open(src_path, encoding="utf-8") as jsn:
                df = pd.read_json(jsn)

            df.to_csv(os.path.join(dest_path, "output.csv"))
            self.log.info("Execution Completed")
            pass

        except Exception as e:
            self.log.error(f"Exception Occurred: {e}")
        
        pass

if __name__ == "__main__":
    # print("Hello world")

    db = DB_Connection()
    # query = "select * from testdb.dbo.mytable;"

    # results = db.execute_fetch_query(query=query, db_engine="mssql")

    # if len(results) > 0:
    #     for i in results:
    #         print(i)
    # else:
    #     print("No Results fetch")

    db.convert_json_to_csv("C:\\tmp\\json-2-csv\\input.json")

    pass