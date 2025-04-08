import json
import os

def normalize_entry(entry):
    """
    Normalise une entrée en détectant son format et en mappant les champs
    vers une structure commune.

    Format 1 attendu (emplois_ma_data_ai_ml_debug.json) :
      - job_url, title, company, description, niveau_etudes, niveau_experience,
        contrat, region, competences, publication_date

    Format 2 attendu (offres_emploi_rekrute.json) :
      - job_title, required_skills, company_description, mission, publication_start,
        publication_end, postes, secteur, fonction, experience, niveau, type_contrat, url
    """
    # Si l'entrée possède "job_url", on considère qu'il s'agit du format 1
    if 'job_url' in entry:
        normalized = {
            "job_url": entry.get("job_url"),
            "title": entry.get("title"),
            "company": entry.get("company"),
            "description": entry.get("description"),
            "niveau_etudes": entry.get("niveau_etudes"),
            "niveau_experience": entry.get("niveau_experience"),
            "contrat": entry.get("contrat"),
            "region": entry.get("region"),
            "competences": entry.get("competences"),
            "publication_date": entry.get("publication_date")
        }
    # Si l'entrée contient "url" ou "job_title", on considère le format 2
    elif 'url' in entry or 'job_title' in entry:
        normalized = {
            "job_url": entry.get("url"),
            "title": entry.get("job_title"),
            # Pour la société, il n'y a pas de champ explicite ; vous pouvez ajuster si besoin
            "company": entry.get("company_description"),
            # On privilégie la mission si elle est présente, sinon on prend les compétences requises
            "description": entry.get("mission") or entry.get("required_skills"),
            # On utilise 'niveau' comme niveau d’études
            "niveau_etudes": entry.get("niveau"),
            # Pour l’expérience, on utilise 'experience'
            "niveau_experience": entry.get("experience"),
            "contrat": entry.get("type_contrat"),
            # Pas de donnée sur la région dans ce format (ajuster si besoin)
            "region": None,
            # On peut mettre les compétences depuis 'required_skills'
            "competences": entry.get("required_skills"),
            # On choisit la date de publication de début
            "publication_date": entry.get("publication_start")
        }
    else:
        # Si le format ne correspond pas aux cas prévus, on renvoie l'entrée sans modification
        normalized = entry
    return normalized

def load_json_file(filepath):
    """Charge un fichier JSON et retourne la liste des objets."""
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Le contenu peut être une liste ou un seul objet
            return data if isinstance(data, list) else [data]
    else:
        raise FileNotFoundError(f"Le fichier {filepath} n'existe pas.")

def remove_duplicates(data, unique_keys):
    """
    Supprime les doublons à partir d'une ou plusieurs clés définissant l'unicité.
    Si une entrée est dupliquée, on fusionne les attributs (la méthode update garde
    en priorité la dernière valeur rencontrée).
    """
    seen = {}
    for item in data:
        key = tuple(item.get(k) for k in unique_keys)
        if key not in seen:
            seen[key] = item
        else:
            seen[key].update(item)
    return list(seen.values())

def merge_files(file1, file2, unique_keys):
    """
    Traite deux fichiers JSON en les normalisant, puis les fusionne
    et supprime les doublons.
    """
    data1 = load_json_file(file1)
    data2 = load_json_file(file2)
    
    normalized_data1 = [normalize_entry(item) for item in data1]
    normalized_data2 = [normalize_entry(item) for item in data2]
    
    # Fusionner les listes normalisées
    all_data = normalized_data1 + normalized_data2
    merged_data = remove_duplicates(all_data, unique_keys)
    return merged_data

# Chemins vers les fichiers d'entrée
file1 = "emplois_ma_data_ai_ml_debug.json"
file2 = "offres_emploi_rekrute.json"

# On choisit "job_url" comme clé d'unicité (vous pouvez adapter si besoin)
unique_keys = ["job_url"]

# Fusion des fichiers
try:
    result = merge_files(file1, file2, unique_keys)
    # Sauvegarder le résultat dans un fichier output.json
    output_file = "merged_jobs.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)
    print(f"Fusion terminée. Le résultat est sauvegardé dans {output_file}.")
except Exception as e:
    print("Une erreur est survenue :", e)
