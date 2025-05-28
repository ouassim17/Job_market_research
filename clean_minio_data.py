import json
import io
from minio import Minio
import pandas as pd

# Connexion à MinIO
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

def clean_data(df):
    df = df.dropna(how='all')  # supprimer lignes vides
    df = df.drop_duplicates()  # supprimer doublons
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # trim des chaînes
    return df

def clean_file(bucket_name, object_name):
    response = client.get_object(bucket_name, object_name)
    data = json.load(io.BytesIO(response.read()))
    df = pd.DataFrame(data)
    df_clean = clean_data(df)
    cleaned_json = df_clean.to_json(orient='records', force_ascii=False, indent=4)

    cleaned_bucket = "cleaned-data"
    if not client.bucket_exists(cleaned_bucket):
        client.make_bucket(cleaned_bucket)

    cleaned_object_name = f"cleaned_{object_name}"
    client.put_object(
        cleaned_bucket,
        cleaned_object_name,
        data=io.BytesIO(cleaned_json.encode('utf-8')),
        length=len(cleaned_json),
        content_type="application/json"
    )
    print(f"✅ Fichier nettoyé et uploadé dans '{cleaned_bucket}' sous le nom '{cleaned_object_name}'")

# Exemple d'utilisation
if __name__ == "__main__":
    bucket = "job-data"
    object_file = "merged_data_profiles.json"  # à adapter
    clean_file(bucket, object_file)
