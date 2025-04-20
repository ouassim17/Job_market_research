#!/usr/bin/env python
import json
import os
import logging
import argparse
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def clean_string(value):
    """Supprime les espaces en début/fin d'une chaîne de caractères."""
    return value.strip() if isinstance(value, str) else value

def parse_date_value(date_str):
    """
    Essaie de parser une date donnée en utilisant plusieurs formats connus.
    Si la date est parsée avec succès, elle est retournée au format ISO (YYYY-MM-DD).
    Pour les formats sans année (défini par une année par défaut de 1900), on remplace
    l'année par l'année en cours.
    Si aucun format ne correspond, la date d'origine est retournée.
    """
    if not date_str or not isinstance(date_str, str):
        return date_str
    formats = [
        "%d/%m/%Y",  # Format d/m/Y
        "%Y/%m/%d",  # Format Y/m/d
        "%Y-%m-%d",  # Format ISO
        "%d-%m-%Y",  # Format d-m-Y
        "%m/%d/%Y",  # Format US
        "%d %b-%H:%M"  # Format pour "10 Apr-10:20"
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    logging.warning(f"Aucun format reconnu pour la date '{date_str}'.")
    return date_str

# Normalisation pour Rekrute
def normalize_rekrute(entry, source):
    normalized = {
        "job_url":clean_string(entry.get("job_url")),
        "title": clean_string(entry.get("job_title")),
        "company": clean_string(entry.get("entreprise")),
        "description": clean_string(entry.get("mission")),
        "niveau_etudes": clean_string(entry.get("niveau")),
        "niveau_experience": clean_string(entry.get("experience")),
        "contrat": clean_string(entry.get("type_contrat")),
        "region": clean_string(entry.get("ville")) or clean_string(entry.get("localisation")),
        "competences": entry.get("required_skills"),
        "publication_date": parse_date_value(entry.get("publication_start")),
        "secteur": clean_string(entry.get("secteur")),
        "salaire": None,
        "domaine": None,
        "via": [source]
    }
    return normalized

# Normalisation pour MarrocAnnonces
def normalize_marroc(entry, source):
    title = clean_string(entry.get("titre"))
    titre_detail = clean_string(entry.get("titre_detail"))
    missions = " ".join([clean_string(item) for item in entry.get("missions", [])])
    description = f"{titre_detail} {missions}".strip()
    region = clean_string(entry.get("ville")) or clean_string(entry.get("localisation"))
    normalized = {
        "job_url":clean_string(entry.get("job_url")),
        "title": title,
        "company": clean_string(entry.get("entreprise")),
        "description": description,
        "niveau_etudes": clean_string(entry.get("niveau_d'études")),
        "niveau_experience": None,
        "contrat": clean_string(entry.get("contrat")),
        "region": region,
        "competences": ", ".join(entry.get("profil_requis", [])),
        "publication_date": parse_date_value(entry.get("date_publication")),
        "secteur": None,
        "salaire": clean_string(entry.get("salaire")),
        "domaine": clean_string(entry.get("domaine")),
        "via": [source]
    }
    return normalized

# Normalisation pour Emplois.ma
def normalize_emploisma(entry, source):
    normalized = {
        "job_url": clean_string(entry.get("job_url")),
        "title": clean_string(entry.get("title")),
        "company": clean_string(entry.get("company")),
        "description": clean_string(entry.get("description")),
        "niveau_etudes": clean_string(entry.get("niveau_etudes")),
        "niveau_experience": clean_string(entry.get("niveau_experience")),
        "contrat": clean_string(entry.get("contrat")),
        "region": clean_string(entry.get("region")),
        "competences": entry.get("competences"),
        "publication_date": parse_date_value(entry.get("publication_date")),
        "secteur": None,
        "salaire": None,
        "domaine": None,
        "via": [source]
    }
    return normalized

# Normalisation pour Bayt
def normalize_bayt(entry, source):
    normalized = {
        "job_url": clean_string(entry.get("job_url")),
        "title": clean_string(entry.get("titre")),
        "company": clean_string(entry.get("companie")),
        "description": clean_string(entry.get("description")),
        "niveau_etudes": None,
        "niveau_experience": None,
        "contrat": None,
        "region": None,
        "competences": clean_string(entry.get("competences")),
        "publication_date": parse_date_value(entry.get("date_publication")),
        "secteur": clean_string(entry.get("intro")),
        "salaire": None,
        "domaine": None,
        "via": [source]
    }
    return normalized

def load_json_file(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Le fichier {filepath} n'existe pas.")
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def remove_duplicates(data, unique_keys):
    seen = {}
    for item in data:
        key = tuple(item.get(k) for k in unique_keys)
        if key not in seen:
            seen[key] = item
        else:
            existing = seen[key]
            existing["via"].extend(item["via"])
            existing["via"] = list(set(existing["via"]))
            for k, v in item.items():
                if k != "via" and v not in (None, ""):
                    existing[k] = v
    return list(seen.values())

def merge_files(file_rekrute, file_marroc, file_emploisma, file_bayt, unique_keys):
    logging.info(f"Chargement de {file_rekrute}")
    data_rekrute = load_json_file(file_rekrute)
    logging.info(f"Chargement de {file_marroc}")
    data_marroc = load_json_file(file_marroc)
    logging.info(f"Chargement de {file_emploisma}")
    data_emploisma = load_json_file(file_emploisma)
    logging.info(f"Chargement de {file_bayt}")
    data_bayt = load_json_file(file_bayt)
    
    normalized_rekrute = [normalize_rekrute(e, "Rekrute") for e in data_rekrute]
    normalized_marroc = [normalize_marroc(e, "MarrocAnnonces") for e in data_marroc]
    normalized_emploisma = [normalize_emploisma(e, "Emplois.ma") for e in data_emploisma]
    normalized_bayt = [normalize_bayt(e, "Bayt") for e in data_bayt]
    
    all_data = normalized_rekrute + normalized_marroc + normalized_emploisma + normalized_bayt
    return remove_duplicates(all_data, unique_keys)

def main():
    parser = argparse.ArgumentParser(description="Fusion de fichiers JSON d'offres d'emploi.")
    parser.add_argument("--file_rekrute", default="offres_emploi_rekrute.json", help="Fichier Rekrute")
    parser.add_argument("--file_marroc", default="offres_marocannonces.json", help="Fichier MarrocAnnonces")
    parser.add_argument("--file_emploisma", default="emplois_ma_data_ai_ml_debug.json", help="Fichier Emplois.ma")
    parser.add_argument("--file_bayt", default="offres_emploi_bayt.json", help="Fichier Bayt")
    parser.add_argument("--unique_keys", nargs="+", default=["title", "publication_date"], help="Clés d'unicité")
    parser.add_argument("--output", default="merged_jobs.json", help="Fichier de sortie")
    
    args = parser.parse_args()
    
    try:
        merged_data = merge_files(
            args.file_rekrute,
            args.file_marroc,
            args.file_emploisma,
            args.file_bayt,
            args.unique_keys
        )
        
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(merged_data, f, indent=4, ensure_ascii=False)
            
        logging.info(f"Fusion réussie ! Résultat sauvegardé dans {args.output}")
        
    except Exception as e:
        logging.error(f"Erreur : {str(e)}")

if __name__ == "__main__":
    main()