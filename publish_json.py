from json.tool import main
import logging
import pandas as pd
import json
import datetime
import os

from dotenv import load_dotenv


class PublishJson:
    def __init__(self) -> None:
        load_dotenv()
        self.log = logging.getLogger(__file__)
        self.mapping_config_file = os.getenv("MAPPING_CONFIG_FILE_PATH")
        self.repeat_entries = 1
        pass


    def get_date(self):
        import pytz
        tz = pytz.timezone("Europe/Berlin")
        return  datetime.datetime.now(tz).strftime("%d-%m-%Y %H:%M:%S")


    def get_filename(self, file_path) -> str:
        return os.path.basename(file_path)

    def get_key_mapping(self, filename):
        """fetch key mapping from config mapping files.
        :param filename: Filename whose mapping configurations should be refered to 
        """
        key_map = dict()
        try:
            with open(self.mapping_config_file, encoding="utf-8") as jsn:
                json_data = json.load(jsn)

            if len(json_data) > 0:
                matching_key = [k for k in json_data.keys() if k in filename]
                if len(matching_key) == 1:
                    key_map = json_data.get(matching_key[0])
                elif len(matching_key) > 1:
                    raise Exception(f"Multiple Keys available in 'json_key_mapping.json' with similar name '{filename}': {matching_key}")
                else:
                    raise Exception(f"Key: {filename}, not available in 'json_key_mapping.json' file.")

        except Exception as e:
            self.log.error(f"get_key_mapping Exception Occurred: {e}")

        return key_map

    def get_json_value(self, json_object: json, json_key: str, file_path: str, order_item_indx=0) -> str:
        value = str()
        data = None
        try:
            if "." in json_key:
                jsn_key_list = json_key.split(".")
                
                for k in jsn_key_list:
                    if k in json_object.keys():
                        data = json_object.get(k)
                    elif isinstance(data, list):
                        self.repeat_entries = len(data)
                        if k in data[order_item_indx].keys():
                            data = data[order_item_indx].get(k)                        
                    else:
                        data = None
            elif  "${" in json_key:
                method_name = json_key.replace("$","").replace("{","").replace("}","").replace("()","")

                if method_name == "getdate":
                    data = self.get_date()
                elif method_name == "getfilename":
                    data = self.get_filename(file_path=file_path)
            else:
                if json_key in json_object.keys():
                    data = json_object.get(json_key)
                else:
                    data = None

            if isinstance(data, str):
                value = data.encode("utf-8")
            elif isinstance(data, dict):
                value = json.dumps(data).encode("utf-8")

        except Exception as e:
            self.log.error(f"get_json_value Exception Ocurred: {e}")
        return value

    def extract_json_data(self, json_file) -> pd.DataFrame:
        """extract data from raw json file
        
        :param json_file: Path to the json file that needs to be extracted.
        """
        df_data = pd.DataFrame()
        json_data = dict()
        tmp_data = dict()
        key_mapping = self.get_key_mapping(self.get_filename(json_file))

        try:
            with open(json_file, encoding="utf-8") as jsn:
                json_data = json.load(jsn)
            
            if len(json_data) > 0:
                order_list = json_data["AmazonEnvelope"]["Message"]

                for order in order_list:
                    self.repeat_entries = 0
                    for k in key_mapping.keys():
                        if k in tmp_data.keys():
                            tmp_data[k].append(self.get_json_value(json_object=order.get("Order"), json_key=key_mapping.get(k), file_path=json_file))
                            if self.repeat_entries > 1:
                                for i in range(1, self.repeat_entries):
                                    tmp_data[k].append(self.get_json_value(json_object=order.get("Order"), json_key=key_mapping.get(k), file_path=json_file, order_item_indx=i))

                        else:
                            tmp_data[k] = [self.get_json_value(json_object=order.get("Order"), json_key=key_mapping.get(k), file_path=json_file)]
            
            df_data = pd.DataFrame(tmp_data)

        except Exception as e:
            self.log.error(f"extract_json_data Exception occured: {e}")

        return df_data

if __name__ == "__main__":

    pj = PublishJson()
    filepath = "./input_files/MAN_GET_XML_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL_20220216_174419068412.json"

    # add below value to .env file
    # MAPPING_CONFIG_FILE_PATH="./config/json_key_mapping.json"
    
    data = pj.extract_json_data(filepath)
    print(data.describe)
    data.to_csv("./input_files/output.csv",index= False)
    # print(pj.get_date())
    # data = "${getfilename()}"
    # method_name = data.replace("$","").replace("{","").replace("}","").replace("()","")
    # print(method_name)