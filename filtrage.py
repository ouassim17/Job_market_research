import json
import os

def normalize_entry(entry, source):
    """
    Normalise une entrée en détectant son format et en mappant les champs
    vers une structure commune, en y ajoutant l'attribut "via" indiquant
    la source des données.
    
    Format 1 attendu (emplois_ma_data_ai_ml_debug.json) :
      - job_url, title, company, description, niveau_etudes, niveau_experience,
        contrat, region, competences, publication_date

    Format 2 attendu (offres_emploi_rekrute.json) :
      - job_title, required_skills, company_description, mission, publication_start,
        publication_end, postes, secteur, fonction, experience, niveau, type_contrat, url
    """
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
            "publication_date": entry.get("publication_date"),
            "via": source
        }
    elif 'url' in entry or 'job_title' in entry:
        normalized = {
            "job_url": entry.get("url"),
            "title": entry.get("job_title"),
            # Pour la société, on prend par défaut "company_description"
            "company": entry.get("company_description"),
            # On privilégie "mission" si présente, sinon "required_skills"
            "description": entry.get("mission") or entry.get("required_skills"),
            "niveau_etudes": entry.get("niveau"),
            "niveau_experience": entry.get("experience"),
            "contrat": entry.get("type_contrat"),
            "region": None,  # Pas de donnée sur la région dans ce format
            "competences": entry.get("required_skills"),
            "publication_date": entry.get("publication_start"),
            "via": source
        }
    else:
        # Si le format ne correspond pas, on renvoie l'entrée sans modification
        # et on pourrait ajouter "via" par défaut
        normalized = entry
        normalized["via"] = source
    return normalized

def load_json_file(filepath):
    """Charge un fichier JSON et retourne la liste des objets."""
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
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
            # Optionnel : si vous souhaitez conserver tous les "via" en cas de doublon,
            # vous pouvez vérifier et fusionner ici, par exemple en stockant une liste.
            seen[key].update(item)
    return list(seen.values())

def merge_files(file1, file2, unique_keys):
    """
    Traite deux fichiers JSON en les normalisant (en ajoutant le champ "via"),
    puis les fusionne et supprime les doublons.
    """
    data1 = load_json_file(file1)
    data2 = load_json_file(file2)
    
    normalized_data1 = [normalize_entry(item, "emplois_ma_data_ai_ml_debug.json") for item in data1]
    normalized_data2 = [normalize_entry(item, "offres_emploi_rekrute.json") for item in data2]
    
    # Fusionner les listes normalisées
    all_data = normalized_data1 + normalized_data2
    merged_data = remove_duplicates(all_data, unique_keys)
    return merged_data

# Chemins vers les fichiers d'entrée
file1 = "emplois_ma_data_ai_ml_debug.json"
file2 = "offres_emploi_rekrute.json"

# On choisit "job_url" comme clé d'unicité (adapter si nécessaire)
unique_keys = ["job_url"]

# Fusion des fichiers
try:
    result = merge_files(file1, file2, unique_keys)
    # Sauvegarde le résultat dans un fichier output.json
    output_file = "merged_jobs.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)
    print(f"Fusion terminée. Le résultat est sauvegardé dans {output_file}.")
except Exception as e:
    print("Une erreur est survenue :", e)
