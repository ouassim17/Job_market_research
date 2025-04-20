import json
import pandas as pd

# Charger et normaliser le JSON
def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # Si les données sont sous la clé "results", extraire cette liste
    if isinstance(data, dict) and 'results' in data:
        data = data['results']
    # S'assurer que c'est bien une liste de dicts
    if not isinstance(data, list):
        raise ValueError("Le JSON doit être une liste ou contenir une clé 'results' renvoyant une liste.")
    return data

# Chemin vers le fichier JSON d'entrée
input_file = 'processed_jobs.json'
# Chemin de sortie pour l'Excel
output_excel = 'processed_jobs.xlsx'
# (Optionnel) Chemin de sortie pour le CSV avec séparateur ';'
output_csv = 'processed_jobs.csv'

# Charger les données
jobs = load_json(input_file)

# Transformer les listes en chaînes séparées par ';'
for job in jobs:
    for k, v in job.items():
        if isinstance(v, list):
            job[k] = ';'.join(map(str, v))
        # Remplacer None par chaîne vide
        if job[k] is None:
            job[k] = ''

# Créer un DataFrame
df = pd.DataFrame(jobs)

# Réorganiser les colonnes selon l'ordre souhaité
field_order = [
    "job_url", "title", "company", "description",
    "niveau_etudes", "niveau_experience", "contrat",
    "region", "competences", "publication_date",
    "via", "titre_homogene", "secteur", "niveau_qualification"
]
# Conserver uniquement les colonnes existantes
columns = [col for col in field_order if col in df.columns]

df = df[columns]

# Exporter vers Excel
# openpyxl est requis (installable via 'pip install openpyxl')
df.to_excel(output_excel, index=False)
print(f"Fichier Excel créé : {output_excel}")

# Exporter également vers CSV avec séparateur ';' si besoin
df.to_csv(output_csv, sep=';', index=False, encoding='utf-8-sig')
print(f"Fichier CSV créé : {output_csv}")
