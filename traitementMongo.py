import os
import json
import logging
import re
import time
from datetime import datetime
from pymongo import MongoClient
import requests
from dotenv import load_dotenv

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("processing.log"), logging.StreamHandler()]
)

# Chargement des variables d'environnement
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

def clean_mongo_document(document):
    """Nettoyage des données MongoDB pour l'API"""
    cleaned = {
        "_id": str(document["_id"]),
        "title": document.get("title", "").strip(),
        "description": (document.get("description", "") or "").strip()[:1000],
        "contrat": (document.get("contrat", "") or "").strip(),
        "region": (document.get("region") or "").strip(),
        "competences": [c.strip() for c in re.split(r'[-,]', (document.get("competences", "") or "")) if c.strip()],
        "niveau_etudes": (document.get("niveau_etudes", "") or "").strip(),
        "niveau_experience": (document.get("niveau_experience", "") or "").strip(),
        "publication_date": document.get("publication_date", ""),
        "job_url": document.get("job_url", "")
    }
    
    # Suppression des valeurs vides
    return {k: v for k, v in cleaned.items() if v not in (None, "", [])}

def prepare_api_payload(offers):
    """Prépare les données pour l'API Groq"""
    return [{
        "title": offer["title"],
       
    } for offer in offers]

def process_with_groq(batch):
    """Envoie un lot à Groq et renvoie les résultats traités"""
    GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
    
    system_prompt = (
        "CLASSIFICATION STRICTE\n"
        "1. Déterminez si l'offre est DATA (ex: Data Scientist, Engineer)\n"
        "2. Générer 'titre_homogene' (ex: 'Data Scientist' pour 'Big Data Engin')\n"
        "3. Déduisez 'secteur' (ex: Data Science, Big Data)\n"
        "4. Déterminez 'niveau_qualification' (1-5)\n"
        "5. Format de réponse : JSON brut sans texte supplémentaire\n"
        "6. Si non-DATA, renvoyez {} pour l'entrée\n"
        "Exemple de réponse valide : [\n"
        "  {\n"
        "    \"titre_homogene\": \"Data Engineer\",\n"
        "    \"secteur\": \"Big Data\",\n"
        "    \"niveau_qualification\": 4\n"
        "  },\n"
        "  {}\n"
        "]"
    )

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "llama3-70b-8192",
        "temperature": 0.0,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(batch, ensure_ascii=False)}
        ]
    }

    try:
        response = requests.post(
            GROQ_API_URL,
            headers=headers,
            json=payload,
            timeout=120
        )
        response.raise_for_status()
        
        # Extraction du JSON
        content = response.json()["choices"][0]["message"]["content"]
        match = re.search(r'```json\s*([\s\S]*?)\s*```', content, re.IGNORECASE)
        if not match:
            match = re.search(r'\[([\s\S]*?)\]', content)
        if match:
            return json.loads("[" + match.group(1) + "]")
        else:
            logging.error(f"Réponse non JSON pour ce lot : {content[:500]}")
            return []
    
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", 10)
            time.sleep(int(retry_after))
            return process_with_groq(batch)  # Réessayer après la pause
        else:
            logging.error(f"Erreur API {response.status_code}: {str(e)}")
            return []
    
    except Exception as e:
        logging.error(f"Erreur critique : {str(e)}")
        return []

def fusionner_offres(existing, new, unique_keys=["title", "company", "job_url"]):
    """Fusionne les offres en évitant les doublons"""
    offres_fusionnees = {}
    for offre in existing + new:
        key = tuple(offre.get(k, "") for k in unique_keys)
        if key in offres_fusionnees:
            # Priorité aux nouvelles données
            existing_offre = offres_fusionnees[key]
            for k, v in offre.items():
                if v not in (None, "", []) and k not in ("via", "titre_homogene", "secteur", "niveau_qualification"):
                    existing_offre[k] = v
            existing_offre["via"] = list(set(existing_offre.get("via", []) + offre.get("via", [])))
        else:
            offres_fusionnees[key] = offre
    return list(offres_fusionnees.values())

def generer_dictionnaire_titres(processed_data):
    """Génère un dictionnaire des titres homogénéisés"""
    dictionnaire_titres = {}
    for offre in processed_data:
        titre = offre.get("titre_homogene", "")
        if titre:
            dictionnaire_titres[titre] = dictionnaire_titres.get(titre, 0) + 1
    return dictionnaire_titres

def main():
    # Connexion MongoDB
    client = MongoClient(MONGO_URI)
    db = client["Data"]
    collection = db["Data(Eng/Scien/An) Jobs"]
    
    # Récupération des données
    cursor = collection.find({})
    raw_data = list(cursor)
    data = [clean_mongo_document(doc) for doc in raw_data]
    
    # Filtrer les offres non-DATA
    filtered_data = []
    for offre in data:
        if any(word in offre["title"].upper() for word in ["DATA", "BIG DATA", "AI", "MACHINE LEARNING"]):
            filtered_data.append(offre)
    
    # Configuration des lots
    batch_size = 3
    cooldown_after_batches = 5
    cooldown_delay = 30  # 30 secondes
    
    results = []
    for i in range(0, len(filtered_data), batch_size):
        batch = filtered_data[i:i+batch_size]
        logging.info(f"Traitement lot {i//batch_size+1}/{len(filtered_data)//batch_size+1}")
        
        # Préparation de la requête
        payload = prepare_api_payload(batch)
        
        # Appel API
        try:
            api_response = process_with_groq(payload)
            if not api_response:
                logging.warning(f"Lot {i} non traité")
                continue
            
            # Vérification de la correspondance entre lots
            if len(api_response) != len(batch):
                logging.error(f"Réponse incomplète pour le lot {i}")
                continue
            
            # Fusion des données
            for original, updated in zip(batch, api_response):
                if updated:
                    original["titre_homogene"] = updated.get("titre_homogene", "")
                    original["secteur"] = updated.get("secteur", "")
                    original["niveau_qualification"] = int(updated.get("niveau_qualification", 0))
                results.append(original)
        
        except Exception as e:
            logging.error(f"Échec traitement lot {i} : {str(e)}")
        
        # Gestion des limites API
        time.sleep(5)
        if (i // batch_size + 1) % cooldown_after_batches == 0:
            time.sleep(cooldown_delay)

    # Générer le dictionnaire des titres
    titre_dict = generer_dictionnaire_titres(results)
    
    # Sauvegarder les résultats
    output = {
        "metadata": {
            "processed_at": datetime.now().isoformat(),
            "total_processed": len(results),
            "model": "llama3-70b-8192"
        },
        "results": results,
        "dictionnaire_titres": titre_dict
    }
    
    try:
        # Sauvegarde des résultats
        with open("processed_jobs.json", 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        # Sauvegarde du dictionnaire des titres
        with open("TitleDic.json", 'w', encoding='utf-8') as f:
            json.dump(titre_dict, f, ensure_ascii=False, indent=2)
        
        logging.info("✅ Traitement terminé avec succès")
    
    except Exception as e:
        logging.error(f"Erreur d'écriture : {str(e)}")

if __name__ == "__main__":
    main()