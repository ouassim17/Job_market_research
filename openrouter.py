#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
one_shot_openrouter_full_debug.py

Ce script :
1. Charge un fichier JSON d’offres d'emploi, structuré en 4 scripts.
2. Pour chaque script (lot), envoie une requête “one-shot” à OpenRouter
   (middle-out + variante extended) pour analyser seulement ce lot.
3. Journalise l'avancement, la réponse brute et toutes les erreurs.
4. Extrait et parse le JSON ARRAY renvoyé, fusionne avec les offres initiales.
5. Concatène tous les résultats et les sauvegarde dans un fichier final JSON.

Prérequis :
    pip install --upgrade requests
    export OPENROUTER_API_KEY="votre_clef_openrouter"
"""

import sys, io, os, json, time, logging, re
from logging.handlers import RotatingFileHandler
import requests

# ─── 0. UTF-8 console sous Windows ─────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ─── 1. Logger ─────────────────────────────────────────────────────────────────────
logger = logging.getLogger("one_shot_pipeline_debug")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout); ch.setLevel(logging.INFO)
fh = RotatingFileHandler("one_shot_pipeline_debug.log", encoding="utf-8", maxBytes=5*1024*1024, backupCount=3); fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s – %(message)s")
ch.setFormatter(fmt); fh.setFormatter(fmt)
logger.addHandler(ch); logger.addHandler(fh)

# ─── 2. Prompt système ───────────────────────────────────────────────────────────
SYSTEM_PROMPT = """
TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. TA MISSION :
1. Détermine si chaque poste est lié aux données (1) ou non (0)
2. Si data_profile=1, catégorise en BI / DATA ENGINEERING / DATA SCIENCE / DATA ANALYST
3. Normalise le niveau d'études (0-5)
4. Normalise l'expérience (années → valeur numérique) et déduit la séniorité (Junior, Senior, Expert)

RENVOIE UNIQUEMENT un JSON ARRAY, commençant par [ et finissant par ].
""".strip()

# ─── Extraction JSON ARRAY ───────────────────────────────────────────────────────
def extract_json_array(text: str) -> str:
    try:
        start = text.index('[')
        end = text.rindex(']') + 1
        return text[start:end]
    except ValueError:
        return None

# ─── Main ─────────────────────────────────────────────────────────────────────────
def main():
    # Charger fichier structuré par scripts
    path = "detailed_offers_by_script.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("Chargé JSON, %d scripts trouvés", len(data))
    except Exception:
        logger.exception("Échec chargement JSON")
        return

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        logger.error("Variable OPENROUTER_API_KEY non définie")
        return
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    model = "deepseek/deepseek-r1:free:extended"

    all_enriched = []
    # Itérer par script pour réduire la taille de chaque requête
    for script_name, content in data.items():
        offers = content.get("old_offers", [])
        if not offers:
            continue
        raw = json.dumps(offers, ensure_ascii=False)
        logger.info("Script '%s': envoi one-shot avec %d offres", script_name, len(offers))

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": raw}
            ],
            "transforms": ["middle-out"],
            "temperature": 0.0
        }
        start = time.perf_counter()
        try:
            resp = requests.post(url, headers=headers, json=payload)
            result = resp.json()
        except Exception:
            logger.exception("HTTP error script %s", script_name)
            continue
        elapsed = time.perf_counter() - start
        logger.info("Réponse script '%s' HTTP %d en %.2fs", script_name, resp.status_code, elapsed)
        logger.debug(json.dumps(result, ensure_ascii=False)[:1000])

        if resp.status_code != 200 or "error" in result:
            err = result.get("error", {})
            logger.error("API error script %s: %s", script_name, err.get("message"))
            continue
        content_text = result["choices"][0]["message"]["content"]
        json_array_str = extract_json_array(content_text)
        if not json_array_str:
            logger.error("Aucun JSON array pour script %s", script_name)
            with open(f"debug_{script_name}.txt", "w", encoding="utf-8") as dbg:
                dbg.write(content_text)
            continue
        try:
            arr = json.loads(json_array_str)
        except json.JSONDecodeError:
            logger.error("Parsing JSON array échoué pour %s", script_name)
            continue
        # Fusion initial+LLM
        for orig, cls in zip(offers, arr):
            merged = {**orig, **cls}
            all_enriched.append(merged)
        time.sleep(1)

    # Sauvegarde final
    out = "one_shot_enriched.json"
    try:
        with open(out, "w", encoding="utf-8") as f:
            json.dump(all_enriched, f, ensure_ascii=False, indent=2)
        logger.info("Sauvegardé total %d offres dans %s", len(all_enriched), out)
    except Exception:
        logger.exception("Échec sauvegarde finale")

if __name__ == "__main__":
    main()
