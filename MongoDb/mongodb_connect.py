#!/usr/bin/env python3
import os
import json
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# 1. Charger MONGO_URI depuis .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("Veuillez définir la variable d'environnement MONGO_URI dans votre .env")

# 2. Configurer les logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()]
)

# 3. Connexion à MongoDB
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
try:
    client.admin.command("ping")
    logging.info("✅ Connecté à MongoDB")
except Exception as e:
    logging.error(f"❌ Impossible de se connecter à MongoDB : {e}")
    raise

db = client["Data"]
collection = db["Webscrapping"]

# 4. Charger le fichier JSON
file_path = "processed_jobs.json"
try:
    with open(file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    logging.info(f"Fichier '{file_path}' chargé ({'liste' if isinstance(payload, list) else 'document unique'})")
except Exception as e:
    logging.error(f"❌ Erreur lors du chargement de '{file_path}' : {e}")
    client.close()
    raise

# 5. Insérer dans la collection
try:
    if isinstance(payload, list):
        result = collection.insert_many(payload)
        logging.info(f"✅ {len(result.inserted_ids)} documents insérés (_ids visibles dans result.inserted_ids)")
    else:
        result = collection.insert_one(payload)
        logging.info(f"✅ Document inséré avec _id={result.inserted_id}")
except PyMongoError as e:
    logging.error(f"❌ Erreur d’insertion : {e}")
    raise
finally:
    client.close()
    logging.info("🔒 Connexion MongoDB fermée")
