import pandas as pd
import json

# Charger et normaliser le JSON
def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extraire les données depuis la clé "results" si nécessaire
    if isinstance(data, dict) and 'results' in data:
        data = data['results']
    
    # Vérifier la structure
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise ValueError("Le fichier JSON doit être une liste de dictionnaires valide.")
    
    return data

# Chemin vers le fichier d'entrée (à adapter)
input_file = 'processed_jobs_demon.json'  

# Charger les données
raw_data = load_json(input_file)

# Convertir en DataFrame pandas
df = pd.json_normalize(raw_data)

# Gérer les tableaux en les convertissant en chaînes (pour Excel)
for col in df.columns:
    if df[col].apply(lambda x: isinstance(x, list)).any():
        df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

# Enregistrer au format XLSX
output_file = "sortie.xlsx"
df.to_excel(output_file, engine="openpyxl", index=False)

print(f"Fichier XLSX généré avec succès : {output_file}")