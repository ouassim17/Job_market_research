import json
import os
from datetime import datetime

# Liste des fichiers à fusionner
input_files = [
    'offres_marocannonces.json',
    'emplois_ma_data_ai_ml_debug.json',
    'offres_emploi_rekrute.json',
    'offres_emploi_bayt.json'
]

# Schéma de sortie attendu
schema_keys = [
    'job_url', 'titre', 'companie', 'description', 'niveau_etudes',
    'niveau_experience', 'contrat', 'region', 'competences', 'secteur',
    'salaire', 'domaine', 'extra', 'via', 'publication_date'
]

# Fonction utilitaire pour normaliser la date en YYYY-MM-DD
def parse_date(date_str):
    if not date_str:
        return ''
    for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    # Si aucun format ne correspond, la renvoyer brute
    return date_str

# Fonction pour charger un JSON et retourner la liste d'objets
def load_json_list(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # S'assurer que c'est une liste
    if isinstance(data, dict) and 'results' in data:
        return data['results']
    if isinstance(data, list):
        return data
    raise ValueError(f"Format inattendu pour {path}")

merged = []

for file in input_files:
    if not os.path.exists(file):
        print(f"Attention : le fichier {file} est introuvable.")
        continue
    items = load_json_list(file)
    for item in items:
        record = {k: '' for k in schema_keys}
        # Mapping simple de champs
        # job_url
        record['job_url'] = item.get('job_url') or item.get('job_url', '') or item.get('job_url')
        # titre
        record['titre'] = item.get('titre') or item.get('title', '')
        # companie
        record['companie'] = item.get('companie') or item.get('company', '')
        # description
        record['description'] = item.get('description', '')
        # niveau_etudes
        record['niveau_etudes'] = item.get('niveau_etudes', '')
        # niveau_experience
        record['niveau_experience'] = item.get('niveau_experience')
        # contrat
        record['contrat'] = item.get('contrat', '')
        # region
        record['region'] = item.get('region', '')
        # competences
        record['competences'] = item.get('competences', '')
        # secteur
        record['secteur'] = item.get('secteur', '')
        # salaire
        salaire = item.get('salaire')
        try:
            record['salaire'] = int(salaire) if salaire is not None and salaire != '' else None
        except (ValueError, TypeError):
            record['salaire'] = None
        # domaine
        record['domaine'] = item.get('domaine', '')
        # extra
        record['extra'] = item.get('intro', '')
        # via
        via = item.get('via')
        if isinstance(via, list):
            record['via'] = ';'.join(via)
        else:
            record['via'] = via or ''
        # publication_date ou date_publication
        raw_date = item.get('publication_date') or item.get('date_publication', '')
        record['publication_date'] = parse_date(raw_date)

        # Ajouter si champs requis présents
        if record['job_url'] and record['titre'] and record['via'] and record['publication_date']:
            merged.append(record)

# Sauvegarde du JSON fusionné
output_file = 'merged_jobs.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(merged, f, ensure_ascii=False, indent=2)

print(f"Fusion terminée : {len(merged)} offres sauvegardées dans {output_file}")
