import os
from minio import Minio
from minio.error import S3Error

# --- Configuration MinIO locale ---
MINIO_URL = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "job-data"
FOLDER_PATH = "./Data_extraction/scraping_output"

client = Minio(
    MINIO_URL,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

# --- Créer le bucket s'il n'existe pas ---
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
    print(f"✅ Bucket '{BUCKET_NAME}' créé.")
else:
    print(f"📦 Bucket '{BUCKET_NAME}' déjà existant.")

# --- Upload de tous les fichiers JSON ---
for filename in os.listdir(FOLDER_PATH):
    if filename.endswith(".json"):
        file_path = os.path.join(FOLDER_PATH, filename)
        object_name = f"scraping_output/{filename}"
        try:
            client.fput_object(
                BUCKET_NAME,
                object_name,
                file_path,
                content_type="application/json"
            )
            print(f"📤 Uploadé : {filename}")
        except S3Error as err:
            print(f"❌ Erreur : {filename} → {err}")
