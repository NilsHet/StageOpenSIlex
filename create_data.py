import pandas as pd
import opensilexClientToolsPython as osCP
from opensilexClientToolsPython import ScientificObjectCreationDTO
import yaml
import numpy as np
import traceback
import json
from datetime import datetime

with open("./params.yaml") as yaml_file:
    params = yaml.safe_load(yaml_file)

# Load the Excel file
df = pd.read_excel(params["file_path2"], engine='openpyxl')  
df = df.fillna(np.nan).replace([np.nan], [None])

# Connect to opensilex
pythonClient = osCP.ApiClient()
try:
    pythonClient.connect_to_opensilex_ws(**params["credentials"])
    print("Connexion réussie à OpenSILEX")
except Exception as e:
    print("Échec de la connexion à OpenSILEX:", str(e))

scientificObjectApi = osCP.ScientificObjectsApi(pythonClient)
germplasmApi = osCP.GermplasmApi(pythonClient)

for index, row in df.iterrows():
    try:
        # Assuming the plantation date is provided in the format 'YYYY'
        plantationYear = row['plantation']
        if plantationYear:
            # Create a datetime object and convert it to ISO 8601 format
            publiDate = datetime(int(plantationYear), 1, 1).isoformat() + 'Z'  # 'Z' indicates UTC time
        else:
            publiDate = None

        Fbunq = str(row["FBUNQ_simple"])
        searchResults = germplasmApi.search_germplasm(code=Fbunq) 
        print(searchResults)

        object_dto = ScientificObjectCreationDTO(
            rdf_type="vocabulary:tree", 
            name=row["Nom"],
            geometry={
                "type": "Point",
                "coordinates": [float(row['long']), float(row['lat'])]
            },
            publication_date=publiDate  
        )

        print('\n', object_dto, '\n')
        #result = scientificObjectApi.create_scientific_object(body=object_dto)
        #print(f"Scientific Object created: {result}")
    except Exception as e:
        print(f"Failed to create scientific object at index {index}: {str(e)}")
        traceback.print_exc()
