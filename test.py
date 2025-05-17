import os
import json
import logging
import time
import random
import requests
import argparse
from datetime import datetime
from dotenv import load_dotenv

# ----------------------------
# Configuration des logs
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"traitement_groq_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ----------------------------
# Chargement des API keys et autres variables
# ----------------------------
load_dotenv()
API_KEYS = [k.strip() for k in os.getenv("GROQ_API_KEYS", "").split(",") if k.strip().startswith("gsk_")]
if not API_KEYS:
    logging.error("Aucune clé API Groq valide trouvée – définissez GROQ_API_KEYS dans .env")
    exit(1)
GROQ_URL = os.getenv("GROQ_API_URL", "https://api.groq.com/openai/v1/chat/completions")

# Iterator cyclique des clés
key_index = 0

def get_api_key():
    global key_index
    key = API_KEYS[key_index]
    current_index = key_index
    key_index = (key_index + 1) % len(API_KEYS)
    return key, current_index

# ----------------------------
# Arguments CLI
# ----------------------------
parser = argparse.ArgumentParser(description="Classification d'offres d'emploi avec Groq API")
parser.add_argument('-i','--input',     required=True, help="Fichier JSON d'entrée")
parser.add_argument('-o','--output',    default=f"classified_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                         help="Fichier JSON de sortie")
parser.add_argument('-b','--batch-size',type=int, default=1, help="Nombre d'offres par requête (par défaut 1)")
parser.add_argument('-p','--pause',     type=int, default=5, help="Pause (s) entre les batches")
parser.add_argument('-m','--model',     default='llama3-70b-8192', help="Modèle Groq à utiliser")
args = parser.parse_args()

# ----------------------------
# Prompt système
# ----------------------------
SYSTEM_PROMPT = """
Tu es un EXPERT EN ANALYSE DE POSTES ET CLASSIFICATION D'EMPLOIS spécialisé dans le domaine des données.

Tu recevras un tableau JSON d'offres (batch).  
Pour chaque offre, renvoie **uniquement** l'objet suivant :
{
  "is_data_profile": 0 ou 1,
  "profile": "...",
  "niveau_etudes": 0-5,
  "niveau_experience": xx,
  "seniorite": "...",
  "competences_techniques": [...],
  "type_contrat": "...",
  "teletravail_possible": true|false,
  "localisation": "..."
}
"""

# ----------------------------
# Utilitaires
# ----------------------------

def load_json(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    logging.info(f"Chargé {len(data)} offres depuis {path}")
    return data


def chunkify(lst, n):
    return [lst[i:i+n] for i in range(0, len(lst), n)]


def extract_json_objects(text):
    results, depth, start = [], 0, None
    for i, ch in enumerate(text):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start is not None:
                candidate = text[start:i+1]
                try:
                    obj = json.loads(candidate)
                    if isinstance(obj, dict):
                        results.append(obj)
                except json.JSONDecodeError:
                    pass
    return results

# ----------------------------
# Gestion des requêtes avec rotation de clé et backoff
# ----------------------------
GLOBAL_429_COUNT = 0
LONG_BACKOFF = 900  # 15 minutes

def process_with_groq(jobs_batch):
    global GLOBAL_429_COUNT
    MAX_WAIT_SECS = 60
    base_wait = 5
    attempt = 0

    payload = {
        'model': args.model,
        'temperature': 0.2,
        'messages': [
            {'role':'system', 'content': SYSTEM_PROMPT},
            {'role':'user',   'content': json.dumps(jobs_batch, ensure_ascii=False)}
        ]
    }

    while True:
        api_key, idx = get_api_key()
        # Afficher quelle clé API est utilisée (affichage partiel pour sécurité)
        logging.info(f"Utilisation de la clé API index {idx} : {api_key[:8]}...{api_key[-4:]} ")
        resp = requests.post(
            GROQ_URL,
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=120
        )

        if resp.status_code == 429:
            attempt += 1
            GLOBAL_429_COUNT += 1
            retry_after = int(resp.headers.get('Retry-After', MAX_WAIT_SECS))
            wait = min(retry_after, base_wait * (2 ** (attempt - 1)), MAX_WAIT_SECS)
            wait += random.uniform(0, 2)
            logging.warning(f"429 avec clé idx {idx} – attente {wait:.1f}s [tentative {attempt}, global {GLOBAL_429_COUNT}]")

            if GLOBAL_429_COUNT >= 20:
                logging.warning(f"Trop de 429. Pause longue {LONG_BACKOFF}s.")
                time.sleep(LONG_BACKOFF)
                GLOBAL_429_COUNT = 0
            else:
                time.sleep(wait)

            if attempt >= 5 or (len(jobs_batch) == 1 and attempt >= 3):
                logging.error(f"Échec batch taille {len(jobs_batch)} après {attempt} retries")
                return []
            continue

        GLOBAL_429_COUNT = 0
        if not resp.ok:
            logging.error(f"Erreur API {resp.status_code}: {resp.text}")
            return []

        text = resp.json()['choices'][0]['message']['content']
        return extract_json_objects(text)

# ----------------------------
# Principal
# ----------------------------

def main():
    all_jobs = load_json(args.input)
    batches  = chunkify(all_jobs, args.batch_size)
    enriched = []

    idx = 0
    while idx < len(batches):
        batch = batches[idx]
        logging.info(f"Batch {idx+1}/{len(batches)} (taille={len(batch)})")
        results = process_with_groq(batch)

        if not results and len(batch) > 1:
            half = len(batch) // 2
            batches.pop(idx)
            batches.insert(idx, batch[half:])
            batches.insert(idx, batch[:half])
            logging.info(f"Split batch {idx+1} en {len(batch[:half])}+{len(batch[half:])}")
            continue

        for job, res in zip(batch, results):
            enriched.append({**job, **res})

        idx += 1
        logging.info(f"Pause {args.pause}s…")
        time.sleep(args.pause)

    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)
    logging.info(f"Terminé : {len(enriched)}/{len(all_jobs)} offres classifiées → {args.output}")

if __name__ == "__main__":
    main()
