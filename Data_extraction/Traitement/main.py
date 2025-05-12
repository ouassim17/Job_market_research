import os
import sys
import json
from datetime import datetime
import pandas as pd

# Define paths
top_dir = os.path.dirname(os.path.abspath(__file__))
scraping_dir = os.path.join(top_dir, "scraping_output")
log_dir = os.path.join(top_dir, "log")
output_dir = os.path.join(top_dir, "output")

os.makedirs(log_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

# Import logger
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from Websites.selenium_init import setup_logger  # noqa

logger = setup_logger(os.path.join(log_dir, "main.log"))

# Map file names to source name
SOURCE_MAP = {
    "offres_emploi_bayt.json": "Bayt",
    "offres_emploi_emploi.json": "Emploi",
    "offres_emploi_rekrute.json": "Rekrute",
    "offres_marocannonces.json": "MarocAnnonces"
}


def load_scraping_files():
    """
    Charge tous les fichiers JSON de scraping_output, ajoute le champ 'via' d'après SOURCE_MAP,
    retourne la liste complète des offres.
    """
    all_offers = []
    if not os.path.isdir(scraping_dir):
        logger.error(f"Scraping directory not found: {scraping_dir}")
        return all_offers

    for fname, via in SOURCE_MAP.items():
        path = os.path.join(scraping_dir, fname)
        if not os.path.isfile(path):
            logger.warning(f"Fichier manquant: {path}")
            continue
        try:
            with open(path, 'r', encoding='utf-8') as f:
                offers = json.load(f)
            if isinstance(offers, list):
                for offer in offers:
                    offer['via'] = via
                all_offers.extend(offers)
                logger.info(f"Loaded {len(offers)} offers from {fname}")
            else:
                logger.warning(f"Contenu inattendu dans {fname}, liste attendue.")
        except Exception as e:
            logger.error(f"Erreur lecture {fname}: {str(e)}")
    logger.info(f"Total offers loaded: {len(all_offers)}")
    return all_offers


def save_backup_and_reports(all_offers):
    """
    Sauvegarde un backup JSON horodaté et un rapport Excel groupé par dates.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(output_dir, f"backup_offers_{timestamp}.json")

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(all_offers, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON backup saved: {json_path}")

    if not all_offers:
        print("Aucune offre à reporter.")
        return

    df = pd.DataFrame(all_offers)
    # Ensure date fields
    for col in ('collect_date', 'publication_date'):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        else:
            logger.warning(f"Colonne manquante: {col}")

    excel_path = os.path.join(output_dir, f"offers_report_{timestamp}.xlsx")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        if 'collect_date' in df.columns:
            for date, group in df.groupby('collect_date'):
                sheet = f"collect_{date.strftime('%Y%m%d')}"
                group.to_excel(writer, sheet_name=sheet, index=False)
        if 'publication_date' in df.columns:
            for date, group in df.groupby('publication_date'):
                sheet = f"pub_{date.strftime('%Y%m%d')}"
                group.to_excel(writer, sheet_name=sheet, index=False)

    logger.info(f"Excel report generated: {excel_path}")
    print(f"Generated JSON: {json_path}\nGenerated Excel: {excel_path}")


if __name__ == "__main__":
    print("Début de la consolidation des données...")
    offers = load_scraping_files()
    save_backup_and_reports(offers)
