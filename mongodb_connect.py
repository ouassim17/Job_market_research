import json
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
<<<<<<< HEAD:Data_extraction/mongodb_connect.py
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

=======
from dotenv import load_dotenv   
# --- Insertion dans MongoDB ---
try:
    load_dotenv()
    uri = os.environ.get("MONGO_DB_URI")
    client = MongoClient(uri, tls=True, tlsAllowInvalidCertificates=False, server_api=ServerApi('1'))
    db = client["DXC"]
    collection = db["rekrute"]
    
    if data:
        result = collection.insert_many(data)
        print(f"{len(result.inserted_ids)} documents insérés dans MongoDB.")
    else:
        print("Aucune donnée à insérer dans MongoDB.")
except Exception as e:
    print("Erreur lors de l'insertion dans MongoDB :", e)
finally:
    client.close()
>>>>>>> e9e7d3c905b645611098e85e73dcc5997c708b51:mongodb_connect.py
