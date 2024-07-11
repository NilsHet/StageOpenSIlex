import pandas as pd
import os

# Path to csv template
TEMPLATE_PHENOLOGICAL_STAGES = './data/csvTemplate/datasetTemplateBBCH.csv'
TEMPLATE_FRUIT_WEIGHTS = './data/csvTemplate/datasetTemplateFruitNumberWeight.csv'

def read_xlsx(file_path):
    """Read the XLSX file and return a DataFrame."""
    return pd.read_excel(file_path)

def read_csv(file_path):
    """Read the CSV template and return a DataFrame."""
    return pd.read_csv(file_path)

def fill_template_phenological_stages(template_df, data_df, column_mapping):
    """Fill the CSV template for phenological stages with data from the XLSX file based on column mapping."""
    # Find the first empty row after the third row
    start_idx = 3  # Start filling from the fourth row

    # Initialize list to collect rows for the output DataFrame
    rows = []

    # Iterate through each row in the input DataFrame
    for _, row in data_df.iterrows():
        target = row['Nom']
        for stage, template_column in column_mapping.items():
            if stage in row.index and pd.notna(row[stage]):
                new_row = {col: None for col in template_df.columns}
                new_row['target'] = target
                new_row['Date'] = pd.to_datetime(row[stage]).strftime('%Y-%m-%d')
                new_row[template_column] = pd.to_datetime(row[stage]).strftime('%Y-%m-%d')
                rows.append(new_row)

    # Create output DataFrame from collected rows
    df_output = pd.DataFrame(rows)

    # Ensure template has enough rows to accommodate data
    required_rows = len(df_output)
    if required_rows > len(template_df) - start_idx:
        new_rows = pd.DataFrame(index=range(required_rows - (len(template_df) - start_idx)), columns=template_df.columns)
        template_df = pd.concat([template_df, new_rows], ignore_index=True)

    # Insert the collected rows starting from the fourth row
    for i, row in df_output.iterrows():
        template_df.iloc[start_idx + i] = row

    return template_df

def fill_template_fruit_number_weights(template_df, data_df, column_mapping):
    """Fill the CSV template for fruit number and weights with data from the XLSX file based on column mapping."""
    # Find the first empty row after the third row
    start_idx = 3  # Start filling from the fourth row

    # Ensure the template_df has enough rows to accommodate data_df
    if len(template_df) - start_idx < len(data_df):
        new_rows = pd.DataFrame(index=range(len(data_df) - (len(template_df) - start_idx)), columns=template_df.columns)
        template_df = pd.concat([template_df, new_rows], ignore_index=True)

    for template_col, xlsx_col in column_mapping.items():
        if template_col == 'target':
            if all(col in data_df.columns for col in ['Geno', 'Ligne', 'Position']):
                template_df.iloc[start_idx:start_idx + len(data_df), template_df.columns.get_loc(template_col)] = data_df.apply(
                    lambda row: f"{row['Geno']}_{row['Ligne']}_{row['Position']}", axis=1
                )
            else:
                print(f"Warning: Columns Geno, Ligne, or Position not found in XLSX file.")
        elif template_col == 'Date' and xlsx_col == 'Year':
            # Transform the year into the desired date format
            data_df[xlsx_col] = data_df[xlsx_col].apply(lambda x: f"{x}-09-01")
            template_df.iloc[start_idx:start_idx + len(data_df), template_df.columns.get_loc(template_col)] = data_df[xlsx_col].values
        elif xlsx_col in data_df.columns:
            template_df.iloc[start_idx:start_idx + len(data_df), template_df.columns.get_loc(template_col)] = data_df[xlsx_col].values
        elif template_col == 'http://opensilex.dev/id/variable/plant_weight50fruits_weighing_kilogramme':
            # Calculate the weight of 50 fruits if both nb_fr_tot and poids_recoltes_tot are available
            if 'nb_fr_tot' in data_df.columns and 'poids_recoltes_tot' in data_df.columns:
                template_df.iloc[start_idx:start_idx + len(data_df), template_df.columns.get_loc(template_col)] = (
                    data_df['poids_recoltes_tot'] / data_df['nb_fr_tot'] * 50
                )
            else:
                print(f"Warning: Columns nb_fr_tot or poids_recoltes_tot not found in XLSX file.")
        else:
            print(f"Warning: Column {xlsx_col} not found in XLSX file.")
    
    return template_df

def save_filled_csv(df, output_path):
    """Save the filled DataFrame to a CSV file."""
    df.to_csv(output_path, index=False)

def process_phenological_stages(xlsx_path, output_dir):
    """Process phenological stages data."""
    # Column mapping for phenological stages
    column_mapping = {
        'stade C': 'http://opensilex.dev/id/variable/plant_bbch54pomefruit_visualobservation_unitless',
        'stade E': 'http://opensilex.dev/id/variable/plant_bbch59pomefruit_visualobservation_unitless',
        'stade F': 'http://opensilex.dev/id/variable/plant_bbch65pomefruit_visualobservation_unitless',
        'stade G': 'http://opensilex.dev/id/variable/plant_bbch67pomefruit_visualobservation_unitless'
    }
    
    return convert_xlsx_to_csv_with_template(xlsx_path, TEMPLATE_PHENOLOGICAL_STAGES, output_dir, column_mapping, fill_template_phenological_stages)

def process_fruit_number_weights(xlsx_path, output_dir):
    """Process fruit weights data."""
    # Column mapping for fruit weights
    column_mapping = {
        'target': 'Geno',
        'Date': 'Year',
        'http://opensilex.dev/id/variable/plant_numberoffruits_calculation_unitless': 'nb_fr_tot',
        'http://opensilex.dev/id/variable/plant_weightharvested_weighing_kilogramme': 'poids_recoltes_tot',
        'http://opensilex.dev/id/variable/plant_weight50fruits_weighing_kilogramme': ''
    }
    
    return convert_xlsx_to_csv_with_template(xlsx_path, TEMPLATE_FRUIT_WEIGHTS, output_dir, column_mapping, fill_template_fruit_number_weights)

def convert_xlsx_to_csv_with_template(xlsx_path, template_path, output_dir, column_mapping, fill_template_function):
    """Main function to convert XLSX to CSV using an existing template and column mapping."""
    # Read input files
    df = read_xlsx(xlsx_path)
    template_df = read_csv(template_path)
    
    # Fill the template with data from XLSX using the provided fill_template function
    filled_df = fill_template_function(template_df, df, column_mapping)
    
    # Create a copy of the template in the output directory
    template_name = os.path.basename(template_path)
    output_base = os.path.join(output_dir, template_name.replace('.csv', '_filled'))
    output_path = output_base + '.csv'
    
    # Handle versioning if the file already exists
    version = 1
    while os.path.exists(output_path):
        output_path = f"{output_base}({version}).csv"
        version += 1
    
    # Save the filled template
    save_filled_csv(filled_df, output_path)
    
    return output_path

def convert_xlsx_to_csv(xlsx_path, output_dir, code):
    """Convert XLSX to CSV based on document code."""
    if code == 1:
        return process_phenological_stages(xlsx_path, output_dir)
    elif code == 2:
        return process_fruit_number_weights(xlsx_path, output_dir)
    else:
        raise ValueError(f"Unsupported code: {code}.")

# Usage
xlsx_path = './data/DebutnomenclatureV3.xlsx'  # Chemin vers le fichier XLSX d'entrée
output_dir = '/home/u108-s786/HetuinNils/download'  # Dossier de sortie pour les fichiers remplis
code = 1  # numéro qui correspond au type de document traité 
# 1 = Document type NomenclatureV3 pour insérer des stade BBCH
# 2 = Document pour envoyé des poids et des nombre de fruits

output_file = convert_xlsx_to_csv(xlsx_path, output_dir, code)
print(f"Filled CSV saved to: {output_file}")