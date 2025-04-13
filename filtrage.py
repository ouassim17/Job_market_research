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

    # Liste de formats à essayer. On ajoute le format pour "10 Apr-10:20".
    formats = [
        "%d/%m/%Y",  # Format typique d/m/Y
        "%Y/%m/%d",  # Format Y/m/d
        "%Y-%m-%d",  # Format ISO
        "%d-%m-%Y",  # Format d-m-Y
        "%m/%d/%Y",  # Format US
        "%d %b-%H:%M"  # Format pour "10 Apr-10:20" (année manquante, renvoie 1900 par défaut)
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Si l'année n'est pas renseignée et reste à 1900, on remplace par l'année courante
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    logging.warning(f"Aucun format reconnu pour la date '{date_str}'.")
    return date_str

# Normalisation pour le format Rekrute (anciennement Recrut)
def normalize_rekrute(entry, source):
    """
    Pour le format Rekrute :
      - Conserve : job_title, required_skills, mission, publication_start, secteur, experience, niveau, type_contrat
      - Supprime : company_description, publication_end, postes, fonction, url
    Le mapping appliqué est :
      • title            <- job_title
      • description      <- mission
      • competences      <- required_skills
      • niveau_etudes    <- niveau
      • niveau_experience<- experience
      • contrat          <- type_contrat
      • publication_date <- publication_start (formaté)
      • secteur          <- secteur
    """
    normalized = {
        "job_url": None,  # L'url est supprimée dans ce format
        "title": clean_string(entry.get("job_title")),
        "company": None,  # company_description est supprimé
        "description": clean_string(entry.get("mission")),
        "niveau_etudes": clean_string(entry.get("niveau")),
        "niveau_experience": clean_string(entry.get("experience")),
        "contrat": clean_string(entry.get("type_contrat")),
        "region": None,  # Non renseigné pour ce format
        "competences": entry.get("required_skills"),
        "publication_date": parse_date_value(entry.get("publication_start")),
        "secteur": clean_string(entry.get("secteur")),
        "salaire": None,
        "domaine": None,
        "via": [source]
    }
    return normalized

# Normalisation pour le format MarrocAnnonces
def normalize_marroc(entry, source):
    """
    Pour le format MarrocAnnonces :
      - Conserve : titre, localisation, titre_detail, date_publication, missions, profil_requis, domaine,
                   contrat, entreprise, salaire, niveau_d'études, ville
      - Supprime : url, localisation_detail, vues, annonce_no, description_intro, fonction, annonceur, téléphone
    Le mapping appliqué est :
      • title            <- titre
      • company          <- entreprise
      • description      <- concatène titre_detail et les éléments de missions s'ils sont présents
      • niveau_etudes    <- niveau_d'études
      • contrat          <- contrat
      • region           <- ville si renseigné, sinon localisation
      • competences      <- profil_requis (fusionné s'il s'agit d'une liste)
      • publication_date <- date_publication (formaté)
      • domaine          <- domaine
      • salaire          <- salaire
    """
    title = clean_string(entry.get("titre"))
    titre_detail = clean_string(entry.get("titre_detail"))
    missions = entry.get("missions")
    missions_text = " ".join([clean_string(item) for item in missions if item]) if isinstance(missions, list) else ""
    description = titre_detail + (" " + missions_text if missions_text else "")
    
    # Pour compétences, joindre les éléments de profil_requis s'il s'agit d'une liste
    profil = entry.get("profil_requis")
    competences = ", ".join([clean_string(item) for item in profil if item]) if isinstance(profil, list) else clean_string(profil)
    
    region = clean_string(entry.get("ville")) or clean_string(entry.get("localisation"))
    
    normalized = {
        "job_url": None,  # L'url est supprimée
        "title": title,
        "company": clean_string(entry.get("entreprise")),
        "description": description if description.strip() else None,
        "niveau_etudes": clean_string(entry.get("niveau_d'études")),
        "niveau_experience": None,  # Non renseigné dans ce format
        "contrat": clean_string(entry.get("contrat")),
        "region": region,
        "competences": competences,
        "publication_date": parse_date_value(entry.get("date_publication")),
        "secteur": None,
        "salaire": clean_string(entry.get("salaire")),
        "domaine": clean_string(entry.get("domaine")),
        "via": [source]
    }
    return normalized

# Normalisation pour le format Emplois.ma
def normalize_emploisma(entry, source):
    """
    Pour le format Emplois.ma (emplois_ma_data_ai_ml_debug.json) :
      On conserve directement les champs :
         job_url, title, company, description, niveau_etudes, niveau_experience,
         contrat, region, competences, publication_date
    """
    normalized = {
        "job_url":  None,
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

def load_json_file(filepath):
    """Charge un fichier JSON et retourne la liste d'objets."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Le fichier {filepath} n'existe pas.")
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data if isinstance(data, list) else [data]

def remove_duplicates(data, unique_keys):
    """
    Supprime les doublons en se basant sur les clés uniques fournies.
    Pour les doublons, fusionne le champ "via" (liste des sources) et, pour les autres
    champs, garde la dernière valeur non nulle.
    """
    seen = {}
    for item in data:
        key = tuple(item.get(k) for k in unique_keys)
        if key not in seen:
            seen[key] = item
        else:
            # Fusionner le champ "via"
            existing_via = seen[key].get("via", [])
            new_via = item.get("via", [])
            seen[key]["via"] = list(set(existing_via + new_via))
            # Pour les autres champs, mettre à jour si la nouvelle valeur est non nulle
            for k, v in item.items():
                if k != "via" and v not in (None, ""):
                    seen[key][k] = v
    return list(seen.values())

def merge_files(file_rekrute, file_marroc, file_emploisma, unique_keys):
    """
    Charge les trois fichiers JSON, les normalise selon leur type, fusionne les données
    et supprime les doublons.
    """
    logging.info(f"Chargement du fichier Rekrute : {file_rekrute}")
    data_rekrute = load_json_file(file_rekrute)
    logging.info(f"Chargement du fichier MarrocAnnonces : {file_marroc}")
    data_marroc = load_json_file(file_marroc)
    logging.info(f"Chargement du fichier Emplois.ma : {file_emploisma}")
    data_emploisma = load_json_file(file_emploisma)
    
    normalized_rekrute = [normalize_rekrute(item, "Rekrute") for item in data_rekrute]
    normalized_marroc = [normalize_marroc(item, "MarrocAnnonces") for item in data_marroc]
    normalized_emploisma = [normalize_emploisma(item, "Emplois.ma") for item in data_emploisma]
    
    all_data = normalized_rekrute + normalized_marroc + normalized_emploisma
    merged_data = remove_duplicates(all_data, unique_keys)
    return merged_data

def main():
    parser = argparse.ArgumentParser(
        description="Fusion et normalisation de 3 fichiers JSON d'offres d'emploi issus de sources différentes."
    )
    # Valeurs par défaut basées sur vos chemins relatifs
    parser.add_argument("--file_rekrute", type=str, default="offres_emploi_rekrute.json", help="Chemin vers le fichier Rekrute")
    parser.add_argument("--file_marroc", type=str, default="offres_marocannonces.json", help="Chemin vers le fichier MarrocAnnonces")
    parser.add_argument("--file_emploisma", type=str, default="emplois_ma_data_ai_ml_debug.json", help="Chemin vers le fichier Emplois.ma")
    # Pour identifier les doublons, on combine "title" et "publication_date"
    parser.add_argument("--unique_keys", nargs="+", default=["title", "publication_date"], help="Clé(s) d'unicité pour fusionner les offres")
    parser.add_argument("--output", type=str, default="merged_jobs.json", help="Fichier de sortie pour les données fusionnées")

    args = parser.parse_args()
    
    try:
        merged = merge_files(args.file_rekrute, args.file_marroc, args.file_emploisma, args.unique_keys)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=4, ensure_ascii=False)
        logging.info(f"Fusion terminée. Le résultat est sauvegardé dans {args.output}.")
    except Exception as e:
        logging.error("Une erreur est survenue : %s", e)

if __name__ == "__main__":
    main()
