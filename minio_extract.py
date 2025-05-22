from minio import Minio
import json

# Connexion à MinIO local
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "job-data"
object_name = "scraping_output/offres_emploi_rekrute.json"

try:
    # Lire le fichier JSON depuis MinIO sans téléchargement
    response = client.get_object(bucket_name, object_name)
    content = response.read()
    data = json.loads(content.decode("utf-8"))

    # Extraire les champs
    print("📌 Résumé des offres :")
    for offre in data:
        print("Titre :", offre.get("titre", "-"))
        print("Entreprise :", offre.get("companie", "-"))
        print("Date :", offre.get("publication_date", "-"))
        print("Lien :", offre.get("job_url", "-"))
        print("-" * 60)

except Exception as e:
    print("❌ Erreur :", e)
