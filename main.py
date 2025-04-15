import os
import subprocess
import logging
import re

# Configuration du logging
logging.basicConfig(
    filename='data_extraction.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_data_extraction_scripts():
    total_offres = 0
    logging.info("Démarrage du processus d'extraction des données.")
    
    # Chemin du dossier Data_extraction
    extraction_dir = os.path.join(os.path.dirname(__file__), 'Data_extraction')
    
    # Vérifier si le dossier existe
    if not os.path.exists(extraction_dir):
        logging.error(f"Le dossier {extraction_dir} n'existe pas!")
        return total_offres

    # Lister tous les fichiers Python dans le dossier
    py_files = [f for f in os.listdir(extraction_dir) if f.endswith('.py')]
    logging.info(f"Scripts trouvés: {py_files}")

    # Exécuter chaque fichier Python
    for py_file in py_files:
        file_path = os.path.join(extraction_dir, py_file)
        logging.info(f"Exécution de {py_file}...")
        
        try:
            result = subprocess.run(
                ['python', file_path],
                check=True,
                capture_output=True,
                text=True
            )
            if result.stdout:
                logging.info(f"Sortie de {py_file}:\n{result.stdout}")
                # Recherche d'une ligne contenant "Nombre d'offres:" suivi d'un nombre.
                match = re.search(r"Nombre d'offres:\s*(\d+)", result.stdout)
                if match:
                    offres_script = int(match.group(1))
                    total_offres += offres_script
                    logging.info(f"Offres extraites par {py_file}: {offres_script}")
                else:
                    logging.warning(f"Aucun nombre d'offres trouvé dans la sortie de {py_file}.")
                
        except subprocess.CalledProcessError as e:
            logging.error(f"Erreur lors de l'exécution de {py_file}: Code {e.returncode} - {e.stderr}")
        except Exception as e:
            logging.exception(f"Erreur inattendue avec {py_file}: {str(e)}")

    logging.info("Fin du traitement de tous les scripts.")
    logging.info(f"Nombre total d'offres extraites: {total_offres}")
    
    return total_offres

if __name__ == "__main__":
    print("Début de l'extraction des données...")
    total = run_data_extraction_scripts()
    print(f"\nTous les scripts d'extraction ont été traités!")
    print(f"Nombre total d'offres extraites: {total}")
