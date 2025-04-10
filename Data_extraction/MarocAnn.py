import json
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, 
    TimeoutException, 
    WebDriverException
)
from selenium_init import init_driver

def extract_offers(driver):
    """
    Extrait les offres affichées sur la page courante.
    Chaque offre est représentée par un dictionnaire contenant le titre, la localisation et l'URL.
    """
    offers_list = []
    holders = driver.find_elements(By.CSS_SELECTOR, "li:not(.adslistingpos) div.holder")
    print(f"Offres trouvées sur cette page : {len(holders)}")
    
    for holder in holders:
        try:
            a_tag = holder.find_element(By.XPATH, "./..")
            job_url = a_tag.get_attribute("href")
            job_title = holder.find_element(By.TAG_NAME, "h3").text.strip()
            location = holder.find_element(By.CLASS_NAME, "location").text.strip()
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

def parse_details_text(text):
    """
    Parse le texte brut récupéré dans le conteneur 'used-cars' afin d'extraire une structure détaillée.
    Retourne un dictionnaire structuré.
    """
    details = {}
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    
    if len(lines) >= 2:
        details["titre_detail"] = lines[0]
        details["localisation_detail"] = lines[1]
    
    for line in lines:
        if line.startswith("Publiée le:"):
            details["date_publication"] = line.replace("Publiée le:", "").strip()
        elif line.startswith("Vue:"):
            details["vues"] = line.replace("Vue:", "").strip()
        elif line.startswith("Annonce N°:"):
            details["annonce_no"] = line.replace("Annonce N°:", "").strip()
    
    text_joined = "\n".join(lines)
    intro_match = re.search(r"Annonce N°:.*\n(.*?)\nMissions :", text_joined, re.DOTALL)
    if intro_match:
        details["description_intro"] = intro_match.group(1).strip()
    
    missions_match = re.search(r"Missions\s*:\s*\n(.*?)\nProfil requis\s*:", text_joined, re.DOTALL)
    if missions_match:
        missions = [m.strip("- ").strip() for m in missions_match.group(1).split("\n") if m.strip()]
        details["missions"] = missions
    
    profil_match = re.search(r"Profil requis\s*:\s*\n(.*?)(Domaine\s*:|$)", text_joined, re.DOTALL)
    if profil_match:
        profil_lines = [p.strip("- ").strip() for p in profil_match.group(1).split("\n") if p.strip()]
        details["profil_requis"] = profil_lines

    fields = ["Domaine", "Fonction", "Contrat", "Entreprise", "Salaire", "Niveau d'études", "Ville"]
    for field in fields:
        pattern = r"{} *: *(.*)".format(field)
        match = re.search(pattern, text_joined)
        if match:
            details[field.lower().replace(" ", "_")] = match.group(1).strip()
    
    try:
        annon_index = lines.index("Annonceur :")
        if annon_index + 1 < len(lines):
            details["annonceur"] = lines[annon_index + 1]
    except ValueError:
        pass
    
    try:
        tel_index = lines.index("Téléphone :")
        if tel_index + 1 < len(lines):
            details["téléphone"] = lines[tel_index + 1]
    except ValueError:
        pass

    return details

def extract_offer_details(driver, offer_url):
    """
    Accède à la page de détail d'une offre, récupère le contenu du conteneur 'div.used-cars'
    et retourne un dictionnaire structuré des informations détaillées.
    """
    details = {}
    try:
        # Limite le temps de chargement de la page pour éviter les blocages prolongés
        driver.set_page_load_timeout(60)
        driver.get(offer_url)
        used_cars_container = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.used-cars"))
        )
        details_text = used_cars_container.text.strip()
        parsed_details = parse_details_text(details_text)
        details.update(parsed_details)
    except TimeoutException:
        print(f"Timeout lors de la récupération des détails pour {offer_url}")
    except WebDriverException as we:
        print(f"WebDriverException pour {offer_url}: {we}")
    except Exception as e:
        print(f"Erreur lors de l'extraction des détails pour {offer_url}: {e}")
    
    return details

def save_json(data, filename="offres_marocannonces.json"):
    """
    Sauvegarde les données extraites dans un fichier JSON.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Données sauvegardées dans {filename}")

def main():
    driver = init_driver()  # Assurez-vous qu'init_driver configure bien le mode headless
    all_offers = []
    
    base_url = "https://www.marocannonces.com/maroc/offres-emploi-b309.html?kw=data+"
    driver.get(base_url)
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
    )
    
    page_num = 1
    while True:
        print(f"Scraping page {page_num}...")
        offers = extract_offers(driver)
        all_offers.extend(offers)
        
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "a.next")
            if next_button:
                next_button.click()
                page_num += 1
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
                )
                # Réduire le temps de pause si possible
                time.sleep(0.5)
            else:
                print("Aucun bouton 'Suivant' trouvé.")
                break
        except Exception as e:
            print("Aucune page suivante trouvée. Fin de la pagination.")
            break
    
    print(f"Total offres extraites : {len(all_offers)}")
    
    for offer in all_offers:
        url = offer.get("url")
        if url:
            print(f"Extraction des détails de l'offre : {url}")
            details = extract_offer_details(driver, url)
            offer.update(details)
            # Réduire la pause entre l'extraction des détails
            time.sleep(0.5)
        else:
            print("URL introuvable pour cette offre, passage à la suivante.")
    
    save_json(all_offers)
    driver.quit()
    print("Extraction terminée !")

if __name__ == "__main__":
    main()
