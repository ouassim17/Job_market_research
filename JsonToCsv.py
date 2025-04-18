<<<<<<< HEAD
import os
import json
import csv
import logging
from pymongo import MongoClient
from dotenv import load_dotenv

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("export.log"), logging.StreamHandler()]
)

# Charger les variables d'environnement
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

def clean_mongo_document(document):
    """Normalise les données pour l'export"""
    cleaned = {
        "_id": str(document["_id"]),
        "title": document.get("title", ""),
        "company": document.get("company", ""),
        "description": (document.get("description", "") or "")[:2000],
        "contrat": document.get("contrat", "").replace("Autre", "").strip(),
        "region": document.get("region", ""),
        "competences": document.get("competences", "").replace(" - ", ",").strip(),
        "publication_date": document.get("publication_date", ""),
        "salaire_mensuel": document.get("salaire_mensuel", {}).get("$numberDouble", 0.0)
    }
    
    # Suppression des valeurs nulles
    return {k: (v if v != "NaN" else 0.0) for k, v in cleaned.items()}

def export_to_json(data):
    """Écriture du fichier JSON"""
    output_file = "mongo_backup.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"✅ Export JSON : {output_file}")

def export_to_csv(data):
    """Écriture du fichier CSV avec séparateur ';'"""
    output_file = "mongo_backup.csv"
    fieldnames = [
        "_id",
        "title",
        "company",
        "description",
        "contrat",
        "region",
        "competences",
        "publication_date",
        "salaire_mensuel"
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for row in data:
            # Conversion des valeurs non-strings en string pour le CSV
            formatted_row = {
                k: str(v).replace("None", "") for k, v in row.items()
            }
            writer.writerow(formatted_row)
    
    logging.info(f"✅ Export CSV : {output_file}")

def main():
    try:
        # Connexion MongoDB
        client = MongoClient(MONGO_URI)
        db = client['Data']
        collection = db['Data(Eng/Scien/An) Jobs']
        
        # Récupération des données
        cursor = collection.find({})
        documents = list(cursor)
        logging.info(f"✅ {len(documents)} documents récupérés")
        
        # Nettoyage des données
        cleaned_data = [clean_mongo_document(doc) for doc in documents]
        
        # Export JSON
        export_to_json(cleaned_data)
        
        # Export CSV
        export_to_csv(cleaned_data)
    
    except Exception as e:
        logging.error(f"❌ Erreur : {str(e)}")
        raise

if __name__ == "__main__":
    main()
=======
import json
import csv

# Charger les données JSON
with open('processed_jobs.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Accéder à la liste selon la clé réelle
data = data["results"]  # ← adapte ce nom selon la structure réelle


# S'assurer que les données sont une liste
if not isinstance(data, list):
    print("Erreur : le fichier JSON ne contient pas une liste d'offres d'emploi.")
    exit()

# Définir les champs à écrire dans le CSV (dans l'ordre souhaité)
fieldnames = [
    "job_url", "title", "company", "description",
    "niveau_etudes", "niveau_experience", "contrat",
    "region", "competences", "publication_date",
    "via", "titre_homogene", "secteur", "niveau_qualification"
]

# Écrire dans le fichier CSV
with open('processed_jobs.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for item in data:
        # Si un champ est manquant, mettre une valeur vide
        row = {key: item.get(key, "") for key in fieldnames}
        writer.writerow(row)

print("Conversion terminée : processed_jobs.csv créé avec succès.")
>>>>>>> e9e7d3c905b645611098e85e73dcc5997c708b51
