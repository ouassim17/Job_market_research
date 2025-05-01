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

def run_script_and_load(script_path: Path):
    """
    Exécute le script dans son dossier (cwd=script_path.parent),
    puis lit et renvoie la liste du fichier JSON qu’il génère.
    """
    script_name = script_path.name
    output_file = script_path.parent / SCRIPT_OUTPUT[script_name]

    try:
        logger.info(f"Démarrage du script {script_name}")
        # on force le cwd sur le dossier du script afin que ses open(...) ciblent ce répertoire
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
    if not output_file.is_file():
        logger.error(f"Fichier de sortie introuvable pour {script_name}: {output_file}")
        return []

    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            logger.info(f"{script_name} a produit {len(data)} offres")
            return data
        else:
            logger.error(f"{script_name} – contenu de {output_file.name} n’est pas une liste")
    except json.JSONDecodeError as je:
        logger.error(f"{script_name} – JSON invalide dans {output_file.name}: {je}")
    except Exception as e:
        logger.error(f"{script_name} – impossible de lire {output_file.name}: {e}")

    return []

def main():
    base_dir       = Path(__file__).resolve().parent
    extraction_dir = base_dir / "Data_extraction"

    # construire la liste des scripts
    scripts = [extraction_dir / name for name in SCRIPT_OUTPUT.keys()]

    # exit si un script manque
    missing = [p for p in scripts if not p.is_file()]
    if missing:
        for p in missing:
            logger.error(f"Script manquant : {p}")
            print(f"Erreur : script introuvable → {p}")
        sys.exit(1)

    all_offers = []
    # exécution parallèle
    with ThreadPoolExecutor(max_workers=len(scripts)) as executor:
        futures = {executor.submit(run_script_and_load, p): p.name for p in scripts}
        for future in as_completed(futures):
            script = futures[future]
            try:
                offers = future.result()
                all_offers.extend(offers)
            except Exception as e:
                logger.error(f"{script} – exception durant la collecte des offres: {e}")

    # écriture du fichier final
    out_file = base_dir / "new_offers.json"
    try:
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(all_offers, f, ensure_ascii=False, indent=4)
        logger.info(f"{len(all_offers)} offres enregistrées dans {out_file.name}")
        print(f"Extraction terminée : {len(all_offers)} offres → {out_file}")
    except Exception as e:
        logger.error(f"Erreur à l’écriture de {out_file.name}: {e}")
        print(f"Erreur à l’écriture de {out_file.name}: {e}")

if __name__ == '__main__':
    main()
