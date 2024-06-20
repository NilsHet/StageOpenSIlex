import create_data
import pandas as pd
import import_datafiles
import opensilexClientToolsPython as osCP
import yaml
import numpy as np


with open("./params.yaml") as yaml_file:
    params = yaml.safe_load(yaml_file)

# Load the Excel file
#df = pd.read_excel(params["file_path"], engine='openpyxl')  
#df = df.fillna(np.nan).replace([np.nan], [None])

# Connect to opensilex
pythonClient = osCP.ApiClient()
try:
    pythonClient.connect_to_opensilex_ws(**params["credentials"])
    print("Connexion réussie à OpenSILEX")
except Exception as e:
    print("Échec de la connexion à OpenSILEX:", str(e))

scientificObjectApi = osCP.ScientificObjectsApi(pythonClient)
germplasmApi = osCP.GermplasmApi(pythonClient)


#base_folder = "/home/u108-s786/HetuinNils/Data/NilsData/Literal/Acquisition/Extraction/2023-04-26/side_1/rgb_images"
#import_datafiles.importImage(base_folder)