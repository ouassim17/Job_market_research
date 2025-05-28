import os
from minio import Minio
from minio.error import S3Error

minio_client = Minio(
    endpoint="localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "job-data"

if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

file_path = input("Entrez le chemin du fichier à uploader (relatif au projet GitHub) : ").strip()

if not os.path.exists(file_path):
    print("Le fichier spécifié n'existe pas.")
else:
    file_name = os.path.basename(file_path)

    try:
        minio_client.fput_object(
            bucket_name,
            file_name,
            file_path,
            content_type="application/octet-stream"
        )
        print(f"Fichier '{file_name}' uploadé avec succès dans le bucket '{bucket_name}'.")
    except S3Error as err:
        print(f"Erreur lors de l'upload : {err}")
