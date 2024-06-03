import pandas as pd
import opensilexClientToolsPython as osCP
import yaml
import numpy as np
import traceback
import json

with open("./params.yaml") as yaml_file:
    params = yaml.safe_load(yaml_file)

    # Load the csv
    df = pd.read_csv(params["file_path"], encoding="unicode_escape")
    df = df.fillna(np.nan).replace([np.nan], [None])

    # Connect to opensilex
    pythonClient = osCP.ApiClient()
    pythonClient.connect_to_opensilex_ws(**params["credentials"])
    print("Connexion réussie à OpenSILEX")
    data_api = osCP.DataApi(pythonClient)


    

    def importImage(df):
        result = []
        for index,row in df.iterrows():

            # Get all the data needed to import the files in opensilex
            dir_path = params["dir_path"]
            file = row[params["columns_mapping"]["file"]]
            file_ext = params["columns_mapping"]["file_ext"]
            file_path = f"{dir_path}/{file}.{file_ext}"
            try:
                new_row = {}
                for key,value in params["columns_mapping"].items():
                    try:
                        if type(value) != dict:
                            new_row[key] = row[value]
                        else:
                            new_row[key] = {
                                key2:row[value2]
                                for key2,value2 in value.items()
                            }
                    except KeyError:
                        pass
                del new_row["file"]
                
                # Import the file in opensilex
                result.append(data_api.post_data_file(
                    # json.dumps or similar MUST be used otherwise the string format isn't right
                    description=json.dumps(new_row), 
                    file=file_path
                )["result"])
            except Exception as e:
                result.append(traceback.format_exc(e))
        print(result)