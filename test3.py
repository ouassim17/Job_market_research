#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import io
import os
import json
import time
import logging
import random
import re
import pandas as pd
from logging.handlers import RotatingFileHandler

# ─── UTF-8 console support ──────────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ─── Logger Setup ───────────────────────────────────────────────────────────────
logger = logging.getLogger("job_classifier")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout); ch.setLevel(logging.INFO)
fh = RotatingFileHandler("job_classifier.log", encoding="utf-8", maxBytes=10*1024*1024, backupCount=5)
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] – %(message)s")
ch.setFormatter(fmt); fh.setFormatter(fmt)
logger.addHandler(ch); logger.addHandler(fh)

# ─── Load .env variables ────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()
API_KEY = os.environ.get("GEMINI_API_KEY")
if not API_KEY:
    logger.error("Variable GEMINI_API_KEY manquante dans .env")
    sys.exit(1)

# ─── Gemini Client Setup ────────────────────────────────────────────────────────
import google.generativeai as genai

# Configure the SDK
genai.configure(api_key=API_KEY)
MODEL = "gemini-1.5-flash"
model = genai.GenerativeModel(MODEL)

# ─── Configuration ──────────────────────────────────────────────────────────────
INPUT_FILE = "Data_extraction/Traitement/output/backup_offers_20250514_214052.json"
BATCH_SIZE = 10
RETRIES = 3
BACKOFF_BASE = 5
MAX_TOKENS = 2048

# ─── Profile Mapping ────────────────────────────────────────────────────────────
PROFILE_MAP = {
    'bi': 'DATA ANALYST',
    'data engineering': 'DATA ENGINEERING',
    'data science': 'DATA SCIENCE',
    'data analyst': 'DATA ANALYST',
    'data scientist': 'DATA SCIENCE',
    'bi developer': 'DATA ENGINEERING',
}

def normalize_profile(raw_profile):
    """Normalise les profils techniques avec fallback"""
    if not raw_profile or not isinstance(raw_profile, str):
        return 'OTHER'
    key = raw_profile.strip().lower()
    return PROFILE_MAP.get(key, re.sub(r"[^\w]", "_", key).upper())

# ─── System Prompt ──────────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "TU ES UN EXPERT EN ANALYSE DE POSTES TECHNIQUES. TA MISSION :\n"
    "1. Détermine si le poste est lié aux données : 1 (oui) ou 0 (non)\n"
    "2. Si data_profile=1, catégorise en : BI / DATA ENGINEERING / DATA SCIENCE / DATA ANALYST\n"
    "3. Extrat le niveau d'expérience en années et déduit la séniorité (Junior, Mid, Senior)\n"
    "4. Extrait les soft skills sous forme de liste\n"
    "5. Extrait les hard skills sous forme de liste\n"
    "RENVOIE UNIQUEMENT un JSON ARRAY avec les clés : "
    "is_data_profile, profile, niveau_experience, soft_skills, hard_skills"
    "\nFORMAT DE SORTIE STRICTEMENT OBLIGATOIRE : [[JSON ARRAY]]"
)

def validate_response(response):
    """Valide la structure du JSON retourné par Gemini"""
    required_keys = {'is_data_profile', 'profile', 'niveau_experience', 'soft_skills', 'hard_skills'}
    try:
        data = json.loads(response)
        if not isinstance(data, list):
            return False
        
        for item in data:
            if not all(key in item for key in required_keys):
                return False
            if not isinstance(item['niveau_experience'], dict):
                return False
        return True
    except json.JSONDecodeError:
        return False

def sanitize_text(text, max_chars=1000):
    """Nettoie et limite la taille du texte"""
    return text[:max_chars].strip() if isinstance(text, str) else ""

def extract_key_skills(description):
    """Extraction basique de compétences en cas d'échec"""
    tech_keywords = ["python", "sql", "java", "docker", "aws", "azure", "gcp", "spark", "hadoop", "scala"]
    soft_keywords = ["équipe", "communication", "autonomie", "résolution", "problèmes"]
    
    return {
        "hard_skills": list(set([kw for kw in tech_keywords if kw in description.lower()])),
        "soft_skills": list(set([kw for kw in soft_keywords if kw in description.lower()]))
    }

def call_gemini_stream(batch):
    """Appelle Gemini avec gestion des erreurs et fallback"""
    sanitized_batch = [
        {k: sanitize_text(v) if isinstance(v, str) else v for k, v in offer.items()}
        for offer in batch
    ]
    prompt = SYSTEM_PROMPT + "\n\n" + json.dumps(sanitized_batch, ensure_ascii=False)
    
    for attempt in range(RETRIES):
        try:
            logger.debug(f"Tentative {attempt+1}/{RETRIES}")
            
            response = model.generate_content(
                prompt,
                generation_config=dict(
                    temperature=0.2,
                    max_output_tokens=MAX_TOKENS,
                    top_p=1,
                    top_k=1,
                )
            )
            content = response.text.strip()
            
            if content.startswith("```json"):
                content = content[7:-3]
                
            if validate_response(content):
                return json.loads(content)
                
            logger.warning("Réponse invalide reçue, tentative de nettoyage...")
            cleaned = re.search(r'\[.*\]', content, re.DOTALL)
            
            if cleaned and validate_response(cleaned.group()):
                return json.loads(cleaned.group())
                
            time.sleep(BACKOFF_BASE * (2 ** attempt) + random.random() * 2)
            
        except Exception as e:
            logger.error(f"Erreur Gemini à la tentative {attempt+1}: {str(e)}")
            time.sleep(BACKOFF_BASE * (2 ** attempt) + random.random() * 2)
    
    # Fallback to individual processing
    logger.warning("Passage au traitement individuel")
    results = []
    
    for offer in batch:
        try:
            single_prompt = SYSTEM_PROMPT + "\n\n" + json.dumps([offer], ensure_ascii=False)
            response = model.generate_content(
                single_prompt,
                generation_config=dict(
                    temperature=0.2,
                    max_output_tokens=MAX_TOKENS,
                    top_p=1,
                    top_k=1,
                )
            )
            
            content = response.text.strip()
            if content.startswith("```json"):
                content = content[7:-3]
                
            if validate_response(content):
                results.extend(json.loads(content))
            else:
                fallback = extract_key_skills(offer.get("description", ""))
                results.append({
                    "is_data_profile": 0,
                    "profile": "OTHER",
                    "niveau_experience": {"min_years": 0, "max_years": 2, "seniority": "junior"},
                    "soft_skills": fallback["soft_skills"],
                    "hard_skills": fallback["hard_skills"]
                })
                
        except Exception as e:
            logger.error(f"Échec traitement individuel: {str(e)}")
            results.append({
                "is_data_profile": 0,
                "profile": "OTHER",
                "niveau_experience": {"min_years": 0, "max_years": 2, "seniority": "junior"},
                "soft_skills": [],
                "hard_skills": []
            })
    
    return results

def main():
    try:
        # Chargement des données
        logger.info(f"Chargement du fichier {INPUT_FILE}")
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            offers = json.load(f)
        
        total = len(offers)
        batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        results = []
        
        logger.info(f"Début du traitement de {total} offres en {batches} batches")

        for i in range(batches):
            start_idx = i * BATCH_SIZE
            end_idx = min((i + 1) * BATCH_SIZE, total)
            batch = offers[start_idx:end_idx]
            
            logger.info(f"Traitement du batch {i+1}/{batches} (offres {start_idx+1}-{end_idx})")
            
            enriched = call_gemini_stream(batch)
            
            # Fusion des résultats
            for orig, upd in zip(batch, enriched):
                # Mise à jour avec fallback
                orig.update({
                    k: v for k, v in upd.items() 
                    if k in ['is_data_profile', 'profile', 'niveau_experience', 'soft_skills', 'hard_skills']
                })
                
                # Normalisation du profil
                orig['profile'] = normalize_profile(orig.get('profile'))
                
                # Valeurs par défaut pour champs critiques
                orig.setdefault('is_data_profile', 0)
                
            results.extend(batch)
            time.sleep(1)  # Légère pause pour éviter les pics de requêtes

        # Sauvegarde finale
        logger.info("Sauvegarde des résultats")
        output_df = pd.DataFrame(results)
        
        output_df.to_excel("enriched_jobs.xlsx", index=False)
        with open("enriched_jobs.json", 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Traitement terminé avec succès: {len(results)} enrichis")

    except Exception as e:
        logger.error(f"Erreur fatale: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()