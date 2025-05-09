<<<<<<< HEAD:JsonToCsv.py
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
=======
import csv
import json

# Charger les données JSON
with open("processed_jobs.json", "r", encoding="utf-8") as f:
    data = json.load(f)
>>>>>>> 731a9834323e3a209b1f9c7c9929f03f49fc4f10:Data_extraction/Traitement/JsonToCsv.py

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

<<<<<<< HEAD:JsonToCsv.py
# Créer un DataFrame
df = pd.DataFrame(jobs)

# Réorganiser les colonnes selon l'ordre souhaité
field_order = [
    "job_url", "title", "company", "description",
    "niveau_etudes", "niveau_experience", "contrat",
    "region", "competences", "publication_date",
    "via", "titre_homogene", "secteur", "niveau_qualification"
=======
# Définir les champs à écrire dans le CSV (dans l'ordre souhaité)
fieldnames = [
    "job_url",
    "title",
    "company",
    "description",
    "niveau_etudes",
    "niveau_experience",
    "contrat",
    "region",
    "competences",
    "publication_date",
    "via",
    "titre_homogene",
    "secteur",
    "niveau_qualification",
>>>>>>> 731a9834323e3a209b1f9c7c9929f03f49fc4f10:Data_extraction/Traitement/JsonToCsv.py
]
# Conserver uniquement les colonnes existantes
columns = [col for col in field_order if col in df.columns]

<<<<<<< HEAD:JsonToCsv.py
df = df[columns]
=======
# Écrire dans le fichier CSV
with open("processed_jobs.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
>>>>>>> 731a9834323e3a209b1f9c7c9929f03f49fc4f10:Data_extraction/Traitement/JsonToCsv.py

# Exporter vers Excel
# openpyxl est requis (installable via 'pip install openpyxl')
df.to_excel(output_excel, index=False)
print(f"Fichier Excel créé : {output_excel}")

# Exporter également vers CSV avec séparateur ';' si besoin
df.to_csv(output_csv, sep=';', index=False, encoding='utf-8-sig')
print(f"Fichier CSV créé : {output_csv}")
