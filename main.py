import os
import sys
import json
import logging
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# — Configuration du logging —
logging.basicConfig(
    filename='data_extraction.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_script(path_to_py: Path):
    """
    Exécute un script Python via subprocess et renvoie l'objet JSON qu'il imprime sur stdout.
    """
    try:
        logging.info(f"Launching {path_to_py.name}")
        # Utiliser sys.executable pour garantir le même interpréteur Python
        result = subprocess.run(
            [sys.executable, str(path_to_py)],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(f"{path_to_py.name} stdout:\n{result.stdout}")
        # On suppose que chaque script imprime un JSON valide sur stdout
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ {path_to_py.name} failed (code {e.returncode}): {e.stderr}")
    except json.JSONDecodeError as e:
        logging.error(f"❌ {path_to_py.name} returned invalid JSON: {e}")
    except Exception:
        logging.exception(f"❌ Unexpected error in {path_to_py.name}")
    return None

def main():
    logging.info("=== Démarrage extraction concurrente ===")

    # Chemin vers le dossier contenant vos scripts
    base_dir = Path(__file__).parent
    extraction_dir = base_dir / "Data_extraction"

    if not extraction_dir.is_dir():
        logging.error(f"Le dossier {extraction_dir} n'existe pas !")
        sys.exit(1)

    # Liste explicite des scripts à lancer
    scripts = [
        extraction_dir / "Rekrute.py",
        extraction_dir / "MarocAnn.py",
        extraction_dir / "bayt.py",
        extraction_dir / "emploi.py",
    ]

    # Vérifier leur existence
    for s in scripts:
        if not s.exists():
            logging.error(f"Script manquant : {s.name}")
            sys.exit(1)

    all_offers = []

    # Exécution concurrente
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_script = {executor.submit(run_script, s): s.name for s in scripts}
        for future in as_completed(future_to_script):
            script_name = future_to_script[future]
            data = future.result()
            if data is None:
                logging.warning(f"Aucun data pour {script_name}")
                continue
            # Si c'est une liste, on l'étend ; si c'est un dict on l'encapsule
            if isinstance(data, list):
                all_offers.extend(data)
            else:
                all_offers.append(data)

    # Sauvegarde finale
    out_file = base_dir / "new_offers.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_offers, f, ensure_ascii=False, indent=4)

    logging.info(f"=== Fin extraction: {len(all_offers)} offres enregistrées dans {out_file} ===")
    print(f"Extraction terminée: {len(all_offers)} offres → {out_file}")

if __name__ == "__main__":
    main()
