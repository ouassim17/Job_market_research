#!/usr/bin/env python3
import logging
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# — Configuration du logger —
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename='data_extraction.log',
    encoding='utf-8',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Pour chaque script, le nom du fichier JSON qu’il génère dans son dossier
SCRIPT_OUTPUT = {
    "Rekrute.py":      "offres_emploi_rekrute.json",
    "MarocAnn.py":     "offres_marocannonces.json",
    "bayt.py":         "offres_emploi_bayt.json",
    "emploi.py":       "emplois_ma_data_ai_ml_debug.json",
}

# Nom du fichier fusionné principal
MAIN_OUTPUT = "new_offers.json"
# Nom du fichier détaillé par script
DETAILED_OUTPUT = "detailed_offers_by_script.json"

def load_json_file(path: Path):
    """
    Charge un fichier JSON s'il existe et retourne son contenu ou [] en cas d'erreur.
    """
    if not path.is_file():
        return []
    try:
        return json.loads(path.read_text(encoding='utf-8')) or []
    except Exception as e:
        logger.error(f"Impossible de charger JSON {path}: {e}")
        return []


def run_script_and_load(script_path: Path):
    """
    Exécute le script dans son dossier (cwd=script_path.parent),
    puis lit et renvoie la liste du fichier JSON qu’il génère.
    """
    script_name = script_path.name
    output_file = script_path.parent / SCRIPT_OUTPUT[script_name]

    try:
        logger.info(f"Démarrage du script {script_name}")
        subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            check=True,
            cwd=script_path.parent
        )
        logger.info(f"{script_name} exécuté avec succès")
    except subprocess.CalledProcessError as e:
        logger.error(f"{script_name} – échec (code {e.returncode}): {e.stderr.strip()}")
        return []
    except Exception as e:
        logger.error(f"{script_name} – erreur inattendue lors de l’exécution: {e}")
        return []

    # lecture du JSON généré dans le dossier du script
    data = load_json_file(output_file)
    if data and isinstance(data, list):
        logger.info(f"{script_name} a produit {len(data)} offres")
        return data
    if data:
        logger.error(f"{script_name} – contenu de {output_file.name} n’est pas une liste")
    else:
        logger.error(f"{script_name} – pas de données dans {output_file.name}")
    return []


def main():
    base_dir       = Path(__file__).resolve().parent
    extraction_dir = base_dir / "Data_extraction"
    main_out_file  = base_dir / MAIN_OUTPUT
    detail_out_file= base_dir / DETAILED_OUTPUT

    # Pré-charger les anciennes offres du main output
    existing_offers = load_json_file(main_out_file)
    if not isinstance(existing_offers, list):
        logger.error(f"{MAIN_OUTPUT} existant n’est pas une liste, repartition vide.")
        existing_offers = []

    # Pré-charger, par script, les anciennes offres individuelles
    old_by_script = {}
    for script_name, fname in SCRIPT_OUTPUT.items():
        path = extraction_dir / fname
        old_by_script[script_name] = load_json_file(path)

    # Préparation des scripts
    scripts = [extraction_dir / name for name in SCRIPT_OUTPUT.keys()]
    missing = [p for p in scripts if not p.is_file()]
    if missing:
        for p in missing:
            logger.error(f"Script manquant : {p}")
            print(f"Erreur : script introuvable → {p}")
        sys.exit(1)

    # Exécution parallèle et collecte des nouvelles offres par script
    new_by_script = {}
    with ThreadPoolExecutor(max_workers=len(scripts)) as executor:
        futures = {executor.submit(run_script_and_load, p): p.name for p in scripts}
        for future in as_completed(futures):
            script = futures[future]
            try:
                new_by_script[script] = future.result() or []
            except Exception as e:
                logger.error(f"{script} – exception durant la collecte des offres: {e}")
                new_by_script[script] = []

    # Concaténer toutes les nouvelles offres
    all_new_offers = []
    for offers in new_by_script.values():
        all_new_offers.extend(offers)

    # Filtrer doublons contre existing_offers
    existing_urls = {off.get('job_url') for off in existing_offers if 'job_url' in off}
    unique_new    = [off for off in all_new_offers if off.get('job_url') not in existing_urls]

    # Fusion finale
    merged_offers = existing_offers + unique_new

    # Sauvegarde main output
    try:
        main_out_file.write_text(json.dumps(merged_offers, ensure_ascii=False, indent=4), encoding='utf-8')
        logger.info(f"{len(unique_new)} nouvelles offres ajoutées — total {len(merged_offers)} dans {MAIN_OUTPUT}")
        print(f"Extraction terminée : {len(unique_new)} nouvelles offres → {MAIN_OUTPUT}")
    except Exception as e:
        logger.error(f"Erreur écriture {MAIN_OUTPUT}: {e}")
        print(f"Erreur écriture {MAIN_OUTPUT}: {e}")

    # Sauvegarde détaillée par script
    detail = {}
    for script in SCRIPT_OUTPUT.keys():
        detail[script] = {
            "old_offers": old_by_script.get(script, []),
            "new_offers": new_by_script.get(script, []),
            "count_old": len(old_by_script.get(script, [])),
            "count_new": len(new_by_script.get(script, []))
        }
    try:
        detail_out_file.write_text(json.dumps(detail, ensure_ascii=False, indent=4), encoding='utf-8')
        logger.info(f"Détail par script écrit dans {DETAILED_OUTPUT}")
    except Exception as e:
        logger.error(f"Erreur écriture {DETAILED_OUTPUT}: {e}")
        print(f"Erreur écriture {DETAILED_OUTPUT}: {e}")

if __name__ == '__main__':
    main()
