
import os
import json
import re
import time
import logging
from datetime import datetime
from collections import Counter
import requests
from dotenv import load_dotenv

# --- 0. Chargement des variables d'environnement ---
load_dotenv()
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_API_URL = os.getenv('GROQ_API_URL')
if not GROQ_API_KEY or not GROQ_API_KEY.startswith('sk-'):
    logging.error("Clé API GROQ non configurée ou invalide dans .env")
    exit(1)
if not GROQ_API_URL:
    logging.error("Variable GROQ_API_URL non définie dans .env")
    exit(1)

# --- 1. Configuration du logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process_jobs.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 2. Fonctions utilitaires ---
def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Erreur lecture {file_path} : {e}")
        exit(1)


def normalize_etudes(value):
    mapping = {
        'bac': 1,
        'bac+2': 2,
        'bac+3': 3,
        'bac+4': 4,
        'bac+5 et plus': 5
    }
    if not isinstance(value, str):
        return 0
    return mapping.get(value.lower().strip(), 0)


def normalize_experience(value):
    if not isinstance(value, str):
        return 0
    nums = re.findall(r'\d+', value)
    return max(map(int, nums)) if nums else 0


def prepare_offer(o):
    # Nettoyage secteur
    secteur = o.get('secteur', '')
    secteurs = [s.strip() for s in re.split(r'[;,]', secteur) if s.strip()]
    # Normalisation compétences
    comp = o.get('competences', '')
    comp_list = [c.strip() for c in re.split(r'[;,]', comp) if c.strip()]
    return {
        'title': o.get('titre', ''),
        'description': o.get('description', ''),
        'secteur': secteurs,
        'competences': comp_list,
        'niveau_etudes': o.get('niveau_etudes', ''),
        'niveau_experience': o.get('niveau_experience', '')
    }


def clean_response(text):
    # Extrait JSON array du texte
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        logger.error("Aucun JSON array trouvé dans la réponse")
        return []
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError as e:
        logger.error(f"Erreur JSONDecode : {e}")
        return []
    valid = []
    required = ['is_data_profile', 'niveau_etudes', 'niveau_experience', 'competences_techniques', 'profile']
    for entry in data:
        if not isinstance(entry, dict):
            logger.error(f"Entrée non valide : {entry}")
            continue
        if not all(k in entry for k in required):
            logger.error(f"Clés manquantes dans {entry}")
            continue
        try:
            entry['is_data_profile'] = int(entry['is_data_profile'])
            entry['niveau_etudes'] = int(entry['niveau_etudes'])
            entry['niveau_experience'] = int(entry['niveau_experience'])
        except ValueError:
            logger.error(f"Conversion int échouée pour {entry}")
            continue
        entry['competences_techniques'] = [c.strip().capitalize() for c in entry.get('competences_techniques', [])]
        entry['profile'] = entry.get('profile', '').upper()
        valid.append(entry)
    return valid


def process_with_groq(batch):
    system_prompt = (
        "TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. "
        "RENVOIE UNIQUEMENT UN JSON ARRAY d'objets avec les clés : "
        "is_data_profile, profile, niveau_etudes, niveau_experience, seniorite, competences_techniques"
    )
    user_messages = []
    for o in batch:
        user_messages.append(
            f"Titre: {o['title']}\nDescription: {o['description'][:1000]}\n"
            f"Secteur: {', '.join(o['secteur'])}\n"
            f"Compétences: {', '.join(o['competences'])}"
        )
    user_content = "\n---\n".join(user_messages)
    payload = {
        'model': 'llama3-8b-8192',
        'temperature': 0.1,
        'messages': [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_content}
        ]
    }
    for attempt in range(3):
        try:
            resp = requests.post(
                GROQ_API_URL,
                headers={
                    'Authorization': f'Bearer {GROQ_API_KEY}',
                    'Content-Type': 'application/json'
                },
                json=payload,
                timeout=60
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get('Retry-After', 5))
                logger.warning(f"Rate limit, retry après {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            content = resp.json().get('choices', [])[0].get('message', {}).get('content', '')
            return clean_response(content)
        except requests.HTTPError as e:
            logger.error(f"HTTP error {resp.status_code}: {resp.text}")
            break
        except Exception as e:
            logger.error(f"Erreur API: {e}")
            break
    return []

# --- 3. Pipeline principal ---
def main():
    input_file = 'merged_jobs.json'
    output_file = 'processed_jobs_demon.json'
    data = load_json(input_file)
    batches = [data[i:i+2] for i in range(0, len(data), 2)]

    results = []
    counter = 0
    for idx, batch in enumerate(batches, 1):
        logger.info(f"Traitement lot {idx}/{len(batches)}: {len(batch)} offres")
        prepared = [prepare_offer(o) for o in batch]
        processed = process_with_groq(prepared)
        if not processed:
            continue
        for orig, upd in zip(batch, processed):
            orig.update({
                'is_data_profile': upd['is_data_profile'],
                'niveau_etudes_normalized': normalize_etudes(orig.get('niveau_etudes', '')),
                'experience_years': normalize_experience(orig.get('niveau_experience', '')),
                'competences_techniques': upd['competences_techniques'],
                'profile': upd['profile']
            })
            results.append(orig)
        counter += 1
        if counter % 5 == 0:
            logger.info("Cooldown 30s après 5 requêtes")
            time.sleep(30)
        time.sleep(3)

    # Stats
    competence_counts = Counter()
    profile_counts = Counter()
    for o in results:
        competence_counts.update(o.get('competences_techniques', []))
        profile_counts.update([o.get('profile', '')])

    output = {
        'metadata': {
            'processed_at': datetime.now().isoformat(),
            'total_processed': len(results),
            'data_profile_count': sum(1 for o in results if o.get('is_data_profile')==1),
            'profile_distribution': dict(profile_counts)
        },
        'results': results,
        'stats': {
            'etudes_dist': dict(Counter(normalize_etudes(o.get('niveau_etudes', '')) for o in results)),
            'experience_dist': dict(Counter(o.get('experience_years') for o in results)),
            'competences_pop': competence_counts.most_common(15)
        }
    }
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info(f"Résultats enregistrés dans {output_file}")

if __name__ == '__main__':
    main()