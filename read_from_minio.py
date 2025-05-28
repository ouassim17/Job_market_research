from minio import Minio
import json
from io import BytesIO

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "job-data"
object_name = "offres_emploi_rekrute.json"

try:
    response = minio_client.get_object(bucket_name, object_name)
    data_bytes = response.read()
    data_str = data_bytes.decode('utf-8')
    json_data = json.loads(data_str)

    for job in json_data:
        print(job.get("titre", "Sans titre"))

    response.close()
    response.release_conn()

except Exception as e:
    print("Erreur :", str(e))
