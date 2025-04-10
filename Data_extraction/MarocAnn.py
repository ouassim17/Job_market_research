import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium_init import init_driver

def extract_offers(driver):
    offers_list = []
    
    # Sélecteur pour cibler les div.holder en excluant les publicités
    holders = driver.find_elements(By.CSS_SELECTOR, "li:not(.adslistingpos) div.holder")
    print(f"Offres trouvées sur cette page : {len(holders)}")
    
    for holder in holders:
        try:
            # Extraction du lien parent
            a_tag = holder.find_element(By.XPATH, "./..")
            job_url = a_tag.get_attribute("href")
            
            # Titre du poste
            job_title = holder.find_element(By.TAG_NAME, "h3").text.strip()
            
            # Localisation
            location = holder.find_element(By.CLASS_NAME, "location").text.strip()
            
            # Création initiale de l'objet offre
            offer = {
                "titre": job_title,
                "localisation": location,
                "url": job_url
            }
            offers_list.append(offer)
            
        except NoSuchElementException as e:
            print(f"Élément non trouvé dans l'offre principale : {e}")
            continue
            
    return offers_list

def extract_offer_details(driver, offer_url):
    """
    Pour une URL donnée, accéder à la page de détails et extraire le contenu du bloc
    situé dans le conteneur ayant les classes 'description desccatemploi'.
    """
    details = {}
    try:
        driver.get(offer_url)
        # Attendre que l'élément contenant la description soit présent
        desc_container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.description.desccatemploi"))
        )
        # Recherche à l'intérieur du conteneur pour le bloc d'informations
        try:
            block_element = desc_container.find_element(By.CSS_SELECTOR, "div.block")
            details_text = block_element.text.strip()
            details["détails"] = details_text
        except NoSuchElementException:
            print(f"Aucun bloc trouvé dans la description pour {offer_url}")
        
    except TimeoutException:
        print(f"Timeout lors de la récupération de la description pour {offer_url}")
    except Exception as e:
        print(f"Erreur lors de l'extraction des détails pour {offer_url} : {e}")
    
    return details

def save_json(data, filename="offres_marocannonces.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Données sauvegardées dans {filename}")

def main():
    driver = init_driver()
    all_offers = []
    
    try:
        # URL de base avec filtre "data"
        base_url = "https://www.marocannonces.com/maroc/offres-emploi-b309.html?kw=data+"
        driver.get(base_url)
        
        # Attendre le chargement des offres
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
        )
        
        # Extraction initiale des offres depuis la page principale
        offers = extract_offers(driver)
        print(f"Total offres extraites (page principale) : {len(offers)}")
        
        # Pour chaque offre, accéder à la page de détail et récupérer les données dans la rubrique 'block'
        for offer in offers:
            url = offer.get("url")
            if url:
                print(f"Extraction des détails de l'offre : {url}")
                details = extract_offer_details(driver, url)
                # Fusionner les détails avec les informations initiales
                offer.update(details)
                # Pause pour éviter des requêtes trop rapides
                time.sleep(1)
            else:
                print("URL de l'offre introuvable, passage à l'offre suivante.")
        
        all_offers.extend(offers)
        print(f"Total offres complètes collectées : {len(all_offers)}")
        
        # Sauvegarde des données extraites
        save_json(all_offers)
        
    except Exception as e:
        print(f"Erreur principale : {e}")
    finally:
        driver.quit()
        print("Extraction terminée !")

if __name__ == "__main__":
    main()
