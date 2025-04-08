from pymongo import MongoClient
from pymongo.server_api import ServerApi
# --- Insertion dans MongoDB ---
try:
    uri = "mongodb+srv://cluster0:Svk8QrZlsoMuSp1n@cluster0.4eopzup.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
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