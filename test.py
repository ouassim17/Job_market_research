import os
import json
import logging
import re
import time
from datetime import datetime
import requests
from dotenv import load_dotenv
from collections import Counter

# Configuration des logs
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
    # Nettoyage des secteurs
    secteur = offer.get("secteur", "")
    secteur = re.sub(r'\s*,\s*', ',', secteur)  # Suppression des espaces autour des virgules
    secteur = list(set(secteur.split(','))) if secteur else []
    
    # Normalisation des compétences
    competences = offer.get("competences", "").lower()
    competences = re.sub(r'[-+]', ' ', competences)  # Suppression des tirets
    competences = [c.strip() for c in competences.split(',') if c.strip()]
    
    return {
        "title": offer.get("titre", ""),
        "description": offer.get("description", ""),
        "secteur": secteur,
        "niveau_etudes": offer.get("niveau_etudes", ""),
        "niveau_experience": offer.get("niveau_experience", ""),
        "competences": competences,
        "domaine": offer.get("domaine", "")
    }

def clean_response(response_text):
    json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```|(\[.*\])'
    matches = re.findall(json_pattern, response_text, re.IGNORECASE | re.DOTALL)
    
    for match in matches:
        json_segment = match[0] or match[1]
        try:
            data = json.loads(json_segment)
            valid_data = []
            for entry in data:
                if not isinstance(entry, dict):
                    logging.error(f"Format incorrect: {entry}")
                    continue
                # Vérification des clés obligatoires
                required_keys = ["is_data_profile", "niveau_etudes", "niveau_experience", "competences_techniques", "profile"]
                if not all(key in entry for key in required_keys):
                    logging.error(f"Clés manquantes: {entry}")
                    continue
                
                # Conversion des types
                entry["is_data_profile"] = int(entry.get("is_data_profile", 0))
                entry["niveau_etudes"] = int(entry.get("niveau_etudes", 0))
                entry["niveau_experience"] = int(entry.get("niveau_experience", 0))
                
                # Nettoyage des compétences
                entry["competences_techniques"] = [
                    c.strip().capitalize() 
                    for c in entry.get("competences_techniques", []) 
                    if c.strip()
                ]
                
                # Validation du profil
                entry["profile"] = entry.get("profile", "NONE").upper()
                if entry["profile"] not in ["BI", "DATA SCIENCE", "DATA ENGINEERING", "DATA ANALYST", "NONE"]:
                    entry["profile"] = "NONE"
                    logging.warning(f"Profil invalide: {entry['profile']}")
                
                valid_data.append(entry)
            return valid_data
        except (json.JSONDecodeError, TypeError):
            logging.error(f"Erreur JSON: {json_segment[:500]}")
    return []

def process_with_groq(batch):
    GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
    max_retries = 3
    
    system_prompt = """
    TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. TA MISSION :
    1. Détermine si le poste est lié aux données (1) ou non (0)
    2. Si data_profile=1, catégorise en :
       - BI (Power BI/Tableau/SQL)
       - DATA ENGINEERING (Spark/Hadoop/Cloud)
       - DATA SCIENCE (Python/R/ML)
       - DATA ANALYST (Excel/SQL/Analytics)
    3. Normalise le niveau d'études (0-5)
    4. Normalise l'expérience (ex: "5-10 ans" → 10)
    
    Format de réponse : JSON ARRAY avec les clés:
    is_data_profile, niveau_etudes, niveau_experience, competences_techniques, profile
    """
    
    user_message = "\n\n".join([
        f"Titre: {o['title']}\n"
        f"Description: {o['description'][:1000]}\n"  # Limite à 1000 caractères
        f"Secteur: {', '.join(o['secteur'])}\n"
        f"Compétences: {', '.join(o['competences'])}\n"
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
    
    return []

def normalize_etudes(value):
    mapping = {
        "": 0,
        "bac": 1,
        "bac+2": 2,
        "bac+3": 3,
        "bac+4": 4,
        "bac+5 et plus": 5
    }
    return mapping.get(value.lower().strip(), 0)

def normalize_experience(value):
    if not value:
        return 0
    numbers = re.findall(r'\d+', value.replace('-', ' ').replace('à', ' '))
    if numbers:
        return max(map(int, numbers))
    return 0

def main():
    input_file = "merged_jobs.json"
    output_file = "processed_jobs_demon.json"
    
    data = load_json(input_file)
    batches = [data[i:i+2] for i in range(0, len(data), 2)]  # Lots de 2 offres
    
    results = []
    request_counter = 0
    
    for i, batch in enumerate(batches, 1):
        logging.info(f"Traitements lot {i}/{len(batches)} - {len(batch)} offres")
        
        # Gestion du cooldown
        if request_counter % 5 == 0 and request_counter != 0:
            logging.info("Cooldown de 30 secondes après 5 requêtes...")
            time.sleep(30)
        
        processed = process_with_groq([prepare_offer(o) for o in batch])
        
        if not processed:
            continue
        
        # Fusion des données originales et traitées
        for original, updated in zip(batch, processed):
            original.update({
                "is_data_profile": updated["is_data_profile"],
                "niveau_etudes_normalized": normalize_etudes(original["niveau_etudes"]),
                "experience_years": normalize_experience(original["niveau_experience"]),
                "competences_techniques": updated["competences_techniques"],
                "profile": updated.get("profile", "NONE").upper()
            })
            results.append(original)
        
        request_counter += 1
        time.sleep(3)  # Pause courte entre les lots
    
    # Statistiques avancées
    competence_counts = Counter()
    profile_counts = Counter()
    
    for offer in results:
        competence_counts.update(offer["competences_techniques"])
        profile_counts.update([offer["profile"]])
    
    output = {
        "metadata": {
            "processed_at": datetime.now().isoformat(),
            "total_processed": len(results),
            "data_profile_count": sum(1 for o in results if o["is_data_profile"] == 1),
            "profile_distribution": dict(profile_counts)
        },
        "results": results,
        "stats": {
            "etudes_dist": Counter([
                normalize_etudes(o["niveau_etudes"]) for o in results
            ]),
            "experience_dist": Counter([
                o["experience_years"] for o in results
            ]),
            "competences_pop": [
                {" ".join(k.split()[:3]): v} 
                for k, v in competence_counts.most_common(15)
                if v >= 2
            ]
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logging.info(f"Résultats enregistrés: {output_file}")

if __name__ == "__main__":
    main()