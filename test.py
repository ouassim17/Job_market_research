#!/usr/bin/env python
import os
import json
import logging
import re
import time
from datetime import datetime
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("traitement.log"),
        logging.StreamHandler()
    ]
)

# Chargement des variables d'environnement
load_dotenv()
API_KEY = os.getenv("GROQ_API_KEY")
if not API_KEY or not API_KEY.startswith("gsk_"):
    logging.error("Clé API Groq non configurée - Vérifiez .env")
    exit(1)

def load_json(file_path: str) -> List[Dict[str, Any]]:
    """Charge le fichier JSON d'entrée."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Erreur de lecture {file_path} : {str(e)}")
        exit(1)

def prepare_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
    """Prépare les données pour l'API en conservant les champs importants."""
    return {
        "title": (offer.get("title", "") or "")[:200],
        "description": (offer.get("description", "") or "")[:1000],
        "competences": [c.strip() for c in (offer.get("competences", "") or "").split("-") if c.strip()]
    }

def clean_response(response_text: str) -> List[Dict[str, Any]]:
    """Extrait et nettoie la réponse JSON depuis la réponse de l'API."""
    json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```|(\[.*\])'
    matches = re.findall(json_pattern, response_text, re.IGNORECASE | re.DOTALL)
    
    for match in matches:
        json_segment = match[0] or match[1]
        try:
            data = json.loads(json_segment)
            for entry in data:
                # Conversion sécurisée de niveau_qualification en entier si possible
                try:
                    entry["niveau_qualification"] = int(entry.get("niveau_qualification", 0))
                except (ValueError, TypeError):
                    entry["niveau_qualification"] = entry.get("niveau_qualification", 0)
                if "competences" in entry and isinstance(entry["competences"], list):
                    entry["competences"] = [c.strip() for c in entry["competences"] if c.strip()]
            return data
        except json.JSONDecodeError:
            logging.error(f"JSONDecodeError pour le segment (limité à 500 caractères) : {json_segment[:500]}")
            continue

    logging.error("Format de réponse non détecté")
    return []

def process_with_groq(batch: List[Dict[str, Any]], dictionary: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Envoie une requête à l'API Groq pour homogénéiser, classifier, enrichir et filtrer les offres
    en se basant sur le domaine "data". Le LLM doit :
      - Vérifier si l'offre appartient bien au domaine DATA (par exemple Data Scientist, Data Engineer, etc.).
      - Ne renvoyer que les offres correspondant au domaine DATA.
      - Homogénéiser le titre et générer le champ 'titre_homogene'.
      - Prédire et compléter les valeurs manquantes (secteur, niveau_qualification, ...).
      - S'appuyer sur le dictionnaire suivant pour classifier les intitulés d'emploi.
    """
    GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
    max_retries = 3
    base_retry_delay = 2

    # Inclusion du dictionnaire dans le prompt
    dict_json = json.dumps(dictionary, ensure_ascii=False, indent=2)
    
    system_prompt = (
        f"""CLASSIFICATION, NORMALISATION, ENRICHISSEMENT ET FILTRAGE DES OFFRES D'EMPLOI
Tu es un expert en RH et en analyse d'offres d'emploi dans le domaine de la DATA.
Tu reçois une liste d'offres d'emploi sous forme de JSON comprenant les champs 'title', 'description' et 'competences'.
Ta mission est de :
- Vérifier si chaque offre appartient réellement au domaine DATA (par exemple Data Scientist, Data Engineer, Analytics, Data Analyst, etc.). Seules les offres DATA doivent être renvoyées.
- Pour les offres DATA, homogénéiser le titre en générant un champ 'titre_homogene' regroupant des intitulés similaires sous un libellé standard.
- Déduire le 'secteur' d'activité et déterminer le 'niveau_qualification' (entier de 1 à 5 : 1 pour Bac, 5 pour Doctorat) en te basant sur les informations disponibles.
- Compléter et prédire les valeurs manquantes dans le contexte de l'offre.
- Utiliser le dictionnaire suivant pour classifier et enrichir les intitulés d'emploi. Pour chaque offre, associe le terme le plus pertinent du dictionnaire si possible.
Le dictionnaire à utiliser est le suivant :
{dict_json}

Réponds uniquement avec un JSON valide, qui est une liste d'objets contenant obligatoirement les champs : 
'title', 'titre_homogene', 'secteur', 'niveau_qualification'.

N'inclus que les offres correspondant au domaine de la DATA.
Exemple de format de sortie :
[
  {{
    "title": "Data Scientist Senior - Paris",
    "titre_homogene": "Data Scientist",
    "secteur": "Data",
    "niveau_qualification": 4
  }}
]"""
    )

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
                        {"role": "user", "content": json.dumps(batch, ensure_ascii=False)}
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
                wait = int(retry_after) if retry_after and retry_after.isdigit() else base_retry_delay * (2 ** attempt)
                logging.warning(f"Trop de requêtes (429) - Pause de {wait} secondes (essai {attempt+1})")
                time.sleep(wait)
                continue
            else:
                logging.error(f"Erreur API (HTTPError) : {str(e)}")
                return []
        except Exception as e:
            logging.error(f"Erreur lors de l'appel API : {str(e)}")
            return []

def fusionner_offres(existing_offers: List[Dict[str, Any]], new_offers: List[Dict[str, Any]], unique_keys: List[str]) -> List[Dict[str, Any]]:
    """
    Fusionne les offres existantes avec les nouvelles offres traitées, en évitant les doublons.
    La clé d'unicité est définie par unique_keys. Si une offre existe déjà, on met à jour ses informations.
    """
    offres_fusionnees = {}
    for offre in existing_offers + new_offers:
        key = tuple(offre.get(k) for k in unique_keys)
        if key in offres_fusionnees:
            offres_fusionnees[key]["via"] = list(set(offres_fusionnees[key].get("via", []) + offre.get("via", [])))
            for k, v in offre.items():
                if k != "via" and v not in (None, ""):
                    offres_fusionnees[key][k] = v
        else:
            offres_fusionnees[key] = offre
    return list(offres_fusionnees.values())

def main():
    # Ici, nous utilisons le même fichier pour les offres à traiter
    input_file = "merged_jobs.json"   # Fichier d'offres existant à enrichir
    output_file = "processed_jobs.json"
    dict_file = "TitleDic.json"
    batch_size = 2                    # Nombre d'offres par lot
    cooldown_after_batches = 3       # Nombre de lots avant une pause longue
    cooldown_delay = 30               # Délai en secondes pour la pause longue

    data = load_json(input_file)
    dictionary = load_json(dict_file)
    
    # Diviser les offres existantes en lots
    batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
    total_batches = len(batches)
    
    results = []
    for i, batch in enumerate(batches, 1):
        logging.info(f"Traitement du lot {i}/{total_batches}")
        prepared_batch = [prepare_offer(offer) for offer in batch]
        processed = process_with_groq(prepared_batch, dictionary)
        
        # Seules les offres DATA (renvoyées par l'API) sont conservées
        if processed:
            for original, updated in zip(batch, processed):
                original.update(updated)
            results.extend(batch)
        else:
            logging.error(f"Le lot {i} n'a pas pu être traité correctement.")
        
        time.sleep(5)  # Pause courte entre chaque lot
        if i % cooldown_after_batches == 0:
            logging.info(f"Cooldown activé : Pause de {cooldown_delay} secondes après {cooldown_after_batches} lots")
            time.sleep(cooldown_delay)
    
    # Fusionner les offres déjà traitées avec les résultats obtenus (suppression des doublons)
    final_results = fusionner_offres(data, results, unique_keys=["title", "publication_date"])
    
    # Générer un dictionnaire récapitulatif des titres homogénéisés
    dictionnaire_titres = {}
    for offer in final_results:
        titre_norm = offer.get("titre_homogene", "").strip()
        if titre_norm:
            dictionnaire_titres[titre_norm] = dictionnaire_titres.get(titre_norm, 0) + 1

    output = {
        "metadata": {
            "processed_at": datetime.now().isoformat(),
            "total_processed": len(final_results),
            "model": "llama3-8b-8192"
        },
        "results": final_results,
        "dictionnaire_titres": dictionnaire_titres
    }
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        logging.info(f"✅ Traitement terminé : {output_file}")
    except Exception as e:
        logging.error(f"Erreur d'écriture : {str(e)}")

if __name__ == "__main__":
    main()
