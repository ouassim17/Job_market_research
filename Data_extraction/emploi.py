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
from selenium_init import init_driver




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

# Sauvegarder localement en JSON pour vérification
with open("emplois_ma_data_ai_ml_debug.json", "w", encoding="utf-8") as f:
    json.dump(jobs, f, ensure_ascii=False, indent=2)
print("Les données ont été sauvegardées dans emplois_ma_data_ai_ml_debug.json")
