import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium_init import highlight, init_driver



# --- Configuration du ChromeDriver ---

driver = init_driver()  # Initialiser le driver avec le chemin du ChromeDriver

jobs = []  # Liste pour stocker les données scrappées

try:
    # Accéder à l'URL de recherche d'offres
    url = "https://www.emploi.ma/recherche-jobs-maroc/data?f%5B0%5D=im_field_offre_metiers%3A31"
    driver.get(url)
    
    # Essayer de localiser le champ de recherche ; parfois le sélecteur peut être différent
    try:
        search_input = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input#keywordSearch"))
        )
        hint = search_input.get_attribute("placeholder")
        search_input.clear()
        search_input.send_keys("DATA AI ML")
        search_input.send_keys(Keys.RETURN)
        print("Requête 'DATA AI ML' soumise.")
    except TimeoutException:
        print("Champ de recherche introuvable, la requête ne sera pas envoyée.")
    
    # Attendre 5 secondes supplémentaires pour que les résultats se chargent
    time.sleep(5)
    
    # Récupérer toutes les cartes d'offres
    cards = driver.find_elements(By.CSS_SELECTOR, "div.card.card-job")
    print("Nombre de cartes trouvées :", len(cards))
    
    for index, card in enumerate(cards, start=1):
        # URL de l'offre
        try:
            job_url = card.get_attribute("data-href").strip() if card.get_attribute("data-href") else ""
        except Exception as e:
            print(f"[Carte {index}] Erreur lors de la récupération de l'URL : {e}")
            job_url = ""
        
<<<<<<< Updated upstream
        # Titre du poste
        try:
            title = card.find_element(By.CSS_SELECTOR, "h3 a").text.strip()
        except NoSuchElementException:
            print(f"[Carte {index}] Titre non trouvé.")
            title = ""
        
        # Nom de l'entreprise
        try:
            company = card.find_element(By.CSS_SELECTOR, "a.card-job-company").text.strip()
        except NoSuchElementException:
            print(f"[Carte {index}] Nom de l'entreprise non trouvé.")
            company = ""
=======
        cards = driver.find_elements(By.CSS_SELECTOR, "div.card.card-job")
       
        print(f"Nombre de cartes trouvées sur la page {page} : {len(cards)}")
        
        # Si aucune carte n'est présente, sortir de la boucle
        if not cards:
            print("Aucune offre trouvée sur cette page, fin de la pagination.")
            break

        for index, card in enumerate(cards, start=1):
            highlight(card , effect_time=0.3)
            # Récupérer l'URL de l'offre
            try:
                job_url = card.get_attribute("data-href").strip() if card.get_attribute("data-href") else ""
            except Exception as e:
                print(f"[Carte {index} - page {page}] Erreur lors de la récupération de l'URL : {e}")
                job_url = ""
            
            # Filtre de doublon basé sur l'URL déjà collectée durant cette session
            if job_url and job_url in collected_urls:
                print(f"[Carte {index} - page {page}] Offre déjà collectée (même URL).")
                continue

            # Récupérer le titre de l'offre
            try:
                title = card.find_element(By.CSS_SELECTOR, "h3 a").text.strip()
            except NoSuchElementException:
                print(f"[Carte {index} - page {page}] Titre non trouvé.")
                title = ""
            
            # Récupérer le nom de l'entreprise
            try:
                company = card.find_element(By.CSS_SELECTOR, "a.card-job-company").text.strip()
            except NoSuchElementException:
                print(f"[Carte {index} - page {page}] Nom de l'entreprise non trouvé.")
                company = ""
            
            # Récupérer la description
            try:
                description = card.find_element(By.CSS_SELECTOR, "div.card-job-description p").text.strip()
            except NoSuchElementException:
                print(f"[Carte {index} - page {page}] Description non trouvée.")
                description = ""
            
            # Informations complémentaires (niveau d'études, expérience, contrat, région, compétences)
            niveau_etudes = ""
            niveau_experience = ""
            contrat = ""
            region = ""
            competences = ""
            try:
                ul = card.find_element(By.CSS_SELECTOR, "div.card-job-detail ul")
                li_elements = ul.find_elements(By.TAG_NAME, "li")
                for li in li_elements:
                    txt = li.text.strip()
                    highlight(txt)
                    if "Niveau d´études requis" in txt or "Niveau d’études requis" in txt:
                        try:
                            niveau_etudes = li.find_element(By.TAG_NAME, "strong").text.strip()
                        except NoSuchElementException:
                            niveau_etudes = ""
                    elif "Niveau d'expérience" in txt:
                        try:
                            niveau_experience = li.find_element(By.TAG_NAME, "strong").text.strip()
                        except NoSuchElementException:
                            niveau_experience = ""
                    elif "Contrat proposé" in txt:
                        try:
                            contrat = li.find_element(By.TAG_NAME, "strong").text.strip()
                        except NoSuchElementException:
                            contrat = ""
                    elif "Région de" in txt:
                        try:
                            region = li.find_element(By.TAG_NAME, "strong").text.strip()
                        except NoSuchElementException:
                            region = ""
                    elif "Compétences clés" in txt:
                        try:
                            competences = li.find_element(By.TAG_NAME, "strong").text.strip()
                        except NoSuchElementException:
                            competences = ""
            except NoSuchElementException:
                print(f"[Carte {index} - page {page}] Section des détails complémentaires non trouvée.")
            
            # Récupérer la date de publication
            try:
                pub_date = card.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime").strip()
            except NoSuchElementException:
                print(f"[Carte {index} - page {page}] Date de publication non trouvée.")
                pub_date = ""
            
            # Vérification : si la date de publication existe déjà dans le fichier, considérer l'offre comme doublon
            if pub_date and pub_date in existing_publication_dates:
                print(f"[Carte {index} - page {page}] Offre déjà existante (date de publication identique: {pub_date}).")
                continue
            
            # Création du dictionnaire de l'offre
            job = {
                "job_url": job_url,
                "title": title,
                "company": company,
                "description": description,
                "niveau_etudes": niveau_etudes,
                "niveau_experience": niveau_experience,
                "contrat": contrat,
                "region": region,
                "competences": competences,
                "publication_date": pub_date
            }
            
            # Ajout de l'offre aux nouvelles offres et mémorisation de l'URL
            new_jobs.append(job)
            if job_url:
                collected_urls.add(job_url)
            # On ajoute également la date dans l'ensemble pour éviter qu'un job ultérieur de même date soit ré-ajouté
            if pub_date:
                existing_publication_dates.add(pub_date)
>>>>>>> Stashed changes
        
        # Description
        try:
            description = card.find_element(By.CSS_SELECTOR, "div.card-job-description p").text.strip()
        except NoSuchElementException:
            print(f"[Carte {index}] Description non trouvée.")
            description = ""
        
        # Informations complémentaires dans la section des détails (extrait du <ul>)
        niveau_etudes = ""
        niveau_experience = ""
        contrat = ""
        region = ""
        competences = ""
        try:
            ul = card.find_element(By.CSS_SELECTOR, "div.card-job-detail ul")
            li_elements = ul.find_elements(By.TAG_NAME, "li")
            for li in li_elements:
                txt = li.text.strip()
                if "Niveau d´études requis" in txt or "Niveau d’études requis" in txt:
                    try:
                        niveau_etudes = li.find_element(By.TAG_NAME, "strong").text.strip()
                    except NoSuchElementException:
                        niveau_etudes = ""
                elif "Niveau d'expérience" in txt:
                    try:
                        niveau_experience = li.find_element(By.TAG_NAME, "strong").text.strip()
                    except NoSuchElementException:
                        niveau_experience = ""
                elif "Contrat proposé" in txt:
                    try:
                        contrat = li.find_element(By.TAG_NAME, "strong").text.strip()
                    except NoSuchElementException:
                        contrat = ""
                elif "Région de" in txt:
                    try:
                        region = li.find_element(By.TAG_NAME, "strong").text.strip()
                    except NoSuchElementException:
                        region = ""
                elif "Compétences clés" in txt:
                    try:
                        competences = li.find_element(By.TAG_NAME, "strong").text.strip()
                    except NoSuchElementException:
                        competences = ""
        except NoSuchElementException:
            print(f"[Carte {index}] Section des détails complémentaires non trouvée.")
        
        # Date de publication
        try:
            pub_date = card.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime").strip()
        except NoSuchElementException:
            print(f"[Carte {index}] Date de publication non trouvée.")
            pub_date = ""
        
        job = {
            "job_url": job_url,
            "title": title,
            "company": company,
            "description": description,
            "niveau_etudes": niveau_etudes,
            "niveau_experience": niveau_experience,
            "contrat": contrat,
            "region": region,
            "competences": competences,
            "publication_date": pub_date
        }
        jobs.append(job)
    
except Exception as e:
    print("Erreur lors du scraping :", e)
finally:
    driver.quit()
    print("Extraction terminée !")

print("Nombre total d'offres extraites :", len(jobs))

<<<<<<< Updated upstream
# Sauvegarder localement en JSON pour vérification
with open("emplois_ma_data_ai_ml_debug.json", "w", encoding="utf-8") as f:
    json.dump(jobs, f, ensure_ascii=False, indent=2)
print("Les données ont été sauvegardées dans emplois_ma_data_ai_ml_debug.json")
=======
# Combinaison des offres existantes et des nouvelles offres
all_jobs = existing_jobs + new_jobs

# Sauvegarde des données en mode "écriture", mais en réécrivant le fichier avec les offres mises à jour
with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(all_jobs, f, ensure_ascii=False, indent=2)
print(f"Les données ont été sauvegardées dans {output_filename} (total des offres : {len(new_jobs) })")
>>>>>>> Stashed changes
