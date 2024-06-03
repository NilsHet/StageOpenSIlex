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
# Connect to opensilex
pythonClient = osCP.ApiClient()
pythonClient.connect_to_opensilex_ws(**params["credentials"])



scientificObjectApi = osCP.ScientificObjectsApi(pythonClient)
germplasmApi = osCP.GermplasmApi(pythonClient)


def createDataFromExcel(df):
    for index, row in df.iterrows():
        try:
            
            plantationYear = row['plantation']
            if plantationYear:
                # Create a datetime object and convert it to ISO 8601 format
                publiDate = datetime(int(plantationYear), 1, 1).isoformat() + 'Z'  # 'Z' indicates UTC time
            else:
                publiDate = None

            Fbunq = str(row["FBUNQ_simple"])
            print(f"Searching for code: {Fbunq}")
            GermplasmUri = getGermplasmFromCode2(Fbunq)
            print(GermplasmUri)

            object_dto = ScientificObjectCreationDTO(
                rdf_type="vocabulary:tree", 
                name=row["Nom"],
                geometry={
                    "type": "Point",
                    "coordinates": [float(row['long']), float(row['lat'])]
                },
                publication_date=publiDate  
            )

            print('\n', '\n')
            #result = scientificObjectApi.create_scientific_object(body=object_dto)
            #print(f"Scientific Object created: {result}")
        except Exception as e:
            print(f"Failed to create scientific object at index {index}: {str(e)}")
            traceback.print_exc()




def getGermplasmFromCode(code):
    
    response = germplasmApi.search_germplasm(code = '689', species="http://aims.fao.org/aos/agrovoc/c_4555")
    return response


def getGermplasmFromCode2(code):
    try:
        response = germplasmApi.search_germplasm() 
        if code in response :
            return response
        else:
            print(f"No germplasm found for code: {code}")
            return None
    except Exception as e:
        print(f"API call failed: {str(e)}")
        traceback.print_exc()
        return None