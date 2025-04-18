import json
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()
MONGO_URI = os.environ["MONGO_URI"]

# Connexion à MongoDB
client = MongoClient(MONGO_URI)
db = client["Data"]
collection = db["Webscrapping"]

def normalize_title(title):
    """Extrait les deux premiers mots et les passe en title case."""
    words = title.split()
    return " ".join(words[:2]).title() if words else ""

def determine_secteur(title):
    """Détermine le secteur à partir du titre (placeholder)."""
    # Ici, on renvoie toujours "Data Science"
    return "Data Science"

def niveau_qualification(niveau_etudes):
    """Mappe le niveau d'études à un niveau numérique."""
    # Exemple de mapping simplifié
    mapping = {
        "Bac+2": 2,
        "Bac+3": 3,
        "Bac+5": 5
    }
    return mapping.get(niveau_etudes, 3)

# Extraction et transformation
normalized_docs = []
for doc in collection.find():
    title = doc.get("job_title", "")
    normalized_docs.append({
        "_id": str(doc["_id"]),
        "title": title,
        "titre_homogene": normalize_title(title),
        "secteur": determine_secteur(title),
        "niveau_qualification": niveau_qualification(doc.get("niveau_etudes", ""))
    })

# Affichage du résultat
print(json.dumps(normalized_docs, ensure_ascii=False, indent=2))

# Optionnel : sauvegarde dans un fichier JSON
with open("normalized_jobs.json", "w", encoding="utf-8") as f:
    json.dump(normalized_docs, f, ensure_ascii=False, indent=2)

client.close()

