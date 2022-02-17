from json.tool import main
import logging
import pandas as pd
import json
import datetime
import os


class PublishJson:
    def __init__(self) -> None:
        self.log = logging.getLogger(__file__)
        pass

    def get_date(self):
        import pytz
        tz = pytz.timezone("Europe/Berlin")
        return  datetime.datetime.now(tz).strftime("%d-%m-%Y %H:%M:%S")

    def get_filename(self, file_path) -> str:
        return os.path.basename(file_path)

    def extract_json_data(self, json_file) -> pd.DataFrame:
        """extract data from raw json file
        
        :param json_file: Path to the json file that needs to be extracted.
        """
        df_data = pd.DataFrame()
        try:
            with open(json_file, encoding="utf-8") as jsn:
                json_data = json.load(jsn)
            
            
            
            pass

        except Exception as e:
            self.log.error(f"Exception occured: {e}")

        return df_data

if __name__ == "__main__":
    pj = PublishJson()
    # filepath = "./input_files/MAN_GET_XML_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL_20220216_174419068412.json"
    # data = pj.extract_json_data(filepath)
    # print(data.describe)
    print(pj.get_date())