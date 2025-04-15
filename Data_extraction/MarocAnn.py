import json
import time
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium_init import init_driver

OUTPUT_FILENAME = "offres_marocannonces.json"

def load_existing_offers(filename):
    """Charge les offres déjà sauvegardées (si le fichier existe)"""
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

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
    Parse le texte brut récupéré dans le conteneur 'used-cars'
    afin d'extraire une structure détaillée.
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
    Accède à la page de détail d'une offre et récupère le contenu du conteneur 'div.used-cars'
    en le structurant via parse_details_text.
    """
    details = {}
    try:
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

def save_json(data, filename=OUTPUT_FILENAME):
    """
    Sauvegarde les données extraites dans un fichier JSON.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Données sauvegardées dans {filename}")

def main():
    # Chargement des offres existantes
    existing_offers = load_existing_offers(OUTPUT_FILENAME)
    # Création d'un ensemble des dates de publication déjà présentes
    existing_pub_dates = {job["date_publication"] for job in existing_offers if "date_publication" in job and job["date_publication"]}
    
    driver = init_driver()  # Initialisation du driver (mode headless si configuré)
    all_offers = []  # Liste temporaire pour les offres collectées dans cette session
    
    # Construction de l'URL de base (pagination)
    base_url = "https://www.marocannonces.com/maroc/offres-emploi-b309.html?kw=data+&pge={}"
    page_num = 1
    while True:
        url = base_url.format(page_num)
        print(f"Scraping page {page_num} : {url}")
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
            )
        except TimeoutException:
            print(f"Timeout lors du chargement de la page {page_num}. Passage à la suivante.")
            break
        except Exception as e:
            print(f"Erreur lors du chargement de la page {page_num} : {e}")
            break

        offers = extract_offers(driver)
        if not offers:
            print("Aucune offre trouvée sur cette page. Fin de la pagination.")
            break
        
        all_offers.extend(offers)
        page_num += 1
        time.sleep(0.5)
    
    print(f"Total offres extraites (avant détails) : {len(all_offers)}")
    
    new_offers = []  # Stockera uniquement les nouvelles offres
    for offer in all_offers:
        offer_url = offer.get("url")
        if offer_url:
            print(f"Extraction des détails de l'offre : {offer_url}")
            details = extract_offer_details(driver, offer_url)
            offer.update(details)
            # Si la nouvelle offre possède une date de publication déjà existante, on passe l'offre
            pub_date = offer.get("date_publication", "")
            if pub_date and pub_date in existing_pub_dates:
                print(f"Offre existante détectée (date: {pub_date}), non ajoutée.")
                continue
            new_offers.append(offer)
            # Mettre à jour l'ensemble pour éviter d'ajouter plusieurs offres avec la même date
            if pub_date:
                existing_pub_dates.add(pub_date)
            time.sleep(0.5)
        else:
            print("URL introuvable pour cette offre, passage à la suivante.")
    
    print(f"Nouvelles offres collectées : {len(new_offers)}")
    
    # Combinaison des offres existantes et des nouvelles offres (uniquement les nouvelles)
    all_jobs = existing_offers + new_offers

    save_json(all_jobs, OUTPUT_FILENAME)
    driver.quit()
    print("Extraction terminée !")

if __name__ == "__main__":
    main()
