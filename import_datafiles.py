import os
import re
import yaml
import json
import traceback
from datetime import datetime
import opensilexClientToolsPython as osCP

# Load parameters
with open("./params.yaml") as yaml_file:
    params = yaml.safe_load(yaml_file)

# Connect to OpenSILEX
pythonClient = osCP.ApiClient()
pythonClient.connect_to_opensilex_ws(**params["credentials"])

data_api = osCP.DataApi(pythonClient)
scientific_objects_api = osCP.ScientificObjectsApi(pythonClient)

def get_images_in_folders(base_folder):
    if not os.path.exists(base_folder):
        print(f"Error: The base folder '{base_folder}' does not exist.")
        return []
    if not os.path.isdir(base_folder):
        print(f"Error: The path '{base_folder}' is not a directory.")
        return []
    
    image_list = []
    for root, _, files in os.walk(base_folder):
        if not files:
            continue
        
        folder_name = os.path.basename(root)
        image_files = [file for file in files if file.lower().endswith(('png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff'))]
        for image_file in image_files:
            image_list.append((folder_name, os.path.join(root, image_file)))

    return image_list

def extract_rang_colonne(folder_name):
    parts = folder_name.split('_')
    return parts[1], parts[3]

def get_scientific_object_name(rang, colonne, scientific_objects_api):
    try:
        search_filter = f"_{rang}_{colonne}"
        regex_pattern = f".*{search_filter}.*"
        search_results = scientific_objects_api.search_scientific_objects(name=regex_pattern)

        if search_results and 'result' in search_results:
            for obj in search_results['result']:
                if hasattr(obj, 'name') and re.match(regex_pattern, obj.name):
                    return obj.uri
        return None
    except Exception as e:
        print(f"Error during scientific object search: {e}")
        print(traceback.format_exc())
        return None

def importImages(base_folder, data_api, scientific_objects_api):
    images = get_images_in_folders(base_folder)
    results = []
    for folder_name, image_path in images:
        try:
            rang, colonne = extract_rang_colonne(folder_name)
            scientific_object_uri = get_scientific_object_name(rang, colonne, scientific_objects_api)
            if scientific_object_uri:
                # Construct description
                
                filename = os.path.basename(image_path)
                description = {
                    "target": scientific_object_uri,
                    "rdf_type": "vocabulary:RGBImage",
                    "provenance": {"uri": "dev:provenance/standard_provenance"},
                    "date": "2024-06-20T12:30:00Z",
                    "file": image_path
                }
                
                # Print the description to verify the content
                print("Description being sent to API:", json.dumps(description, indent=2))
                
                # Debugging: Check if the image file exists
                if not os.path.isfile(image_path):
                    print(f"Error: File {image_path} does not exist.")
                    continue
                
                # Print the file path being sent
                print(f"Sending file: {image_path}")
                
                result = data_api.post_data_file(
                    description=json.dumps(description),
                    file=image_path
                )["result"]
                
                print(f"Successfully sent image {image_path} for object {scientific_object_uri}")
                results.append(result)
            else:
                print(f"No scientific object found for folder {folder_name}")
        except Exception as e:
            print(f"Error processing folder {folder_name}: {e}")
            print(traceback.format_exc())
    
    print(results)

base_folder = "/home/u108-s786/HetuinNils/Data/NilsData/Literal/Acquisition/Extraction/2023-04-26/side_1/rgb_images"
importImages(base_folder, data_api, scientific_objects_api)
