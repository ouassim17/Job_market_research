import os
import json
import logging
import re
import time
from datetime import datetime
import requests
from dotenv import load_dotenv
from collections import Counter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("traitement_demon.log"), logging.StreamHandler()]
)

load_dotenv()
API_KEY = os.getenv("GROQ_API_KEY")
if not API_KEY or not API_KEY.startswith("gsk_"):
    logging.error("Clé API Groq non configurée - Vérifiez .env")
    exit(1)

def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Erreur lecture {file_path}: {str(e)}")
        exit(1)

def prepare_offer(offer):
    return {
        "title": offer.get("titre", ""),
        "description": offer.get("description", ""),
        "secteur": offer.get("secteur", ""),
        "niveau_etudes": offer.get("niveau_etudes", ""),
        "niveau_experience": offer.get("niveau_experience", ""),
        "competences": offer.get("competences", ""),
        "domaine": offer.get("domaine", "")
    }

def clean_response(response_text):
    json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```|(\[.*\])'
    matches = re.findall(json_pattern, response_text, re.IGNORECASE | re.DOTALL)
    
    for match in matches:
        json_segment = match[0] or match[1]
        try:
            data = json.loads(json_segment)
            for entry in data:
                entry["is_data_profile"] = int(entry.get("is_data_profile", 0))
                entry["niveau_etudes"] = int(entry.get("niveau_etudes", 0))
                entry["niveau_experience"] = int(entry.get("niveau_experience", 0))
                entry["competences_techniques"] = [
                    c.strip() for c in entry.get("competences_techniques", []) if c.strip()
                ]
                entry["profile"] = entry.get("profile", "NONE").upper()
            return data
        except (json.JSONDecodeError, TypeError):
            logging.error(f"Erreur JSON: {json_segment[:500]}")
    return []

def process_with_groq(batch):
    GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
    max_retries = 3
    
    system_prompt = """
    TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. TA MISSION :
    1. Détermine si le poste est lié aux données (1) ou non (0)
    2. Si c'est un profil data, catégorise en :
       - BI (outils: Power BI, Tableau, SQL Server)
       - DATA SCIENCE (Python, R, Machine Learning)
       - DATA ENGINEERING (Apache Spark, Hadoop, SQL)
       - DATA ANALYST (Excel, SQL, Data Visualization)
    3. Normalise le niveau d'études (0-5)
    4. Normalise l'expérience en années (ex: "3-5 ans" → 4)
    5. Extrait les compétences techniques (outils/technos)
    
    Format de réponse : JSON ARRAY avec les clés:
    is_data_profile (0/1),
    niveau_etudes (0-5),
    niveau_experience (0-20),
    competences_techniques (array),
    profile (BI/DATA SCIENCE/DATA ENGINEERING/DATA ANALYST)
    """
    
    user_message = "\n\n".join([
        f"Titre: {o['title']}\n"
        f"Description: {o['description']}\n"
        f"Secteur: {o['secteur']}\n"
        f"Domaine: {o['domaine']}\n"
        f"Niveau études: {o['niveau_etudes']}\n"
        f"Expérience: {o['niveau_experience']}\n"
        f"Compétences: {o['competences']}"
        for o in batch
    ])
    
    for attempt in range(max_retries):
        try:
            response = requests.post(
                GROQ_API_URL,
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "llama3-8b-8192",
                    "temperature": 0.1,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ]
                },
                timeout=60
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return clean_response(content)
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < max_retries - 1:
                retry_after = response.headers.get("Retry-After")
                wait = int(retry_after) if retry_after else 2 ** (attempt + 2)
                logging.warning(f"Trop de requêtes (429) - Attente de {wait} secondes")
                time.sleep(wait)
            else:
                logging.error(f"Erreur API ({response.status_code}): {response.text}")
                return []
        except (KeyError, json.JSONDecodeError) as e:
            logging.error(f"Réponse API invalide: {str(e)}")
            return []
        except Exception as e:
            logging.error(f"Erreur inattendue: {str(e)}")
            return []
    
    return []

def normalize_etudes(value):
    mapping = {
        "": 0,
        "Bac": 1,
        "Bac +2": 2,
        "Bac +3": 3,
        "Bac +4": 4,
        "Bac +5 et plus": 5
    }
    return mapping.get(value, 0)

def normalize_experience(value):
    if not value:
        return 0
    numbers = re.findall(r'\d+', value)
    if numbers:
        return sum(map(int, numbers)) // len(numbers)
    return 0

def main():
    input_file = "merged_jobs.json"
    output_file = "processed_jobs_demon.json"
    
    
    data = load_json(input_file)
    batches = [data[i:i+2] for i in range(0, len(data), 2)]  # Lots de 2 offres
    
    results = []
    request_counter = 0  # Compteur de requêtes
    
    for i, batch in enumerate(batches, 1):
        logging.info(f"Traitements lot {i}/{len(batches)}")
        
        # Gestion du cooldown après chaque 5 requêtes
        if request_counter % 5 == 0 and request_counter != 0:
            logging.info("Cooldown de 30 secondes après 5 requêtes...")
            time.sleep(30)
        
        processed = process_with_groq([prepare_offer(o) for o in batch])
        # ... reste du code de traitement ...
        
        request_counter += 1  # Incrémentation après chaque requête
        
        time.sleep(3)  # Pause courte entre les lots
    
    # Statistiques avancées
    competence_counts = Counter()
    for offer in results:
        competence_counts.update(offer["competences_techniques"])
    
    output = {
        "metadata": {
            "processed_at": datetime.now().isoformat(),
            "total_processed": len(results),
            "data_profile_count": sum(1 for o in results if o["is_data_profile"] == 1),
            "profile_distribution": {
                "BI": sum(1 for o in results if o["profile"] == "BI"),
                "DATA SCIENCE": sum(1 for o in results if o["profile"] == "DATA SCIENCE"),
                "DATA ENGINEERING": sum(1 for o in results if o["profile"] == "DATA ENGINEERING"),
                "DATA ANALYST": sum(1 for o in results if o["profile"] == "DATA ANALYST")
            }
        },
        "results": results,
        "stats": {
            "etudes_dist": Counter([o["niveau_etudes_normalized"] for o in results]),
            "experience_dist": Counter([o["experience_years"] for o in results]),
            "competences_pop": [{" ".join(k.split()[:3]): v} for k, v in competence_counts.most_common(10)]
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logging.info(f"Résultats enregistrés: {output_file}")

if __name__ == "__main__":
    main()