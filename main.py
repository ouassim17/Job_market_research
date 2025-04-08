import os
import subprocess

def run_data_extraction_scripts():
    # Chemin du dossier Data_extraction
    extraction_dir = os.path.join(os.path.dirname(__file__), 'Data_extraction')
    
    # Vérifier si le dossier existe
    if not os.path.exists(extraction_dir):
        print(f"Le dossier {extraction_dir} n'existe pas!")
        return

    # Lister tous les fichiers Python dans le dossier
    py_files = [f for f in os.listdir(extraction_dir) if f.endswith('.py')]

    # Exécuter chaque fichier Python
    for py_file in py_files:
        file_path = os.path.join(extraction_dir, py_file)
        print(f"\nExécution de {py_file}...")
        
        try:
            # Exécuter le script avec l'interpréteur Python actuel
            result = subprocess.run(
                ['python', file_path],
                check=True,
                capture_output=True,
                text=True
            )
            
            # Afficher la sortie du script
            if result.stdout:
                print(f"Sortie de {py_file}:\n{result.stdout}")
                
        except subprocess.CalledProcessError as e:
            print(f"Erreur lors de l'exécution de {py_file}:")
            print(f"Code d'erreur: {e.returncode}")
            print(f"Erreur: {e.stderr}")
        except Exception as e:
            print(f"Erreur inattendue avec {py_file}: {str(e)}")

if __name__ == "__main__":
    print("Début de l'extraction des données...")
    run_data_extraction_scripts()
    print("\nTous les scripts d'extraction ont été traités!")