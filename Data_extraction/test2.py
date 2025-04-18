import json
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (NoSuchElementException, TimeoutException,
                                        ElementClickInterceptedException, ElementNotInteractableException)
from selenium_init import init_driver

def normalize_header(header, header_keywords):
    header = header.lower().strip()
    for norm, variations in header_keywords.items():
        if any(header.startswith(v) for v in variations):
            return norm
    return header  # fallback en cas de non-correspondance

def text_segmentation(job_offer_details):
    """
    Découpe le texte complet d'une offre en sections à partir des headers connus.
    Renvoie un dictionnaire avec au moins une section 'intro'
    et éventuellement 'job_description', 'skills', 'preferred_candidate' et 'company'.
    """
    header_keywords = {
        'job_description': ['job description', 'description'],
        'skills': ['skills', 'required skills'],
        'preferred_candidate': ['preferred candidate', 'candidate profile'],
        'company': ['company', 'about the company']
    }
    all_keywords = [kw for group in header_keywords.values() for kw in group]
    regex_pattern = r'\n(?=({}))'.format('|'.join(map(re.escape, all_keywords)))
    
    sections = re.split(regex_pattern, job_offer_details, flags=re.IGNORECASE)
    parsed_sections = {}
    parsed_sections['intro'] = sections[0].strip()
    
    for i in range(1, len(sections), 2):
        header = sections[i]
        content = sections[i+1] if i+1 < len(sections) else ''
        key = normalize_header(header, header_keywords)
        parsed_sections[key] = content.strip()
    return parsed_sections

def access_bayt(driver):
    """
    Accède à la page de Bayt et effectue une recherche sur "DATA".
    """
    base_url = "https://www.bayt.com/en/morocco/"
    driver.get(base_url)
    search_input = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input#text_search"))
    )
    search_input.clear()
    search_input.send_keys("DATA" + Keys.RETURN)
    print("Requête 'DATA' soumise sur Bayt.")

def extract_job_details(driver, offers=[]):
    """
    Extrait les détails d'une offre de job :
      - Titre du job,
      - Nom de l'entreprise,
      - Date de publication (à partir d'un sélecteur ou du texte),
      - Détail complet découpé en sections (intro, job_description, skills, etc.)
    """
    try:
        job_title = driver.find_element(By.CSS_SELECTOR, "h2#jobViewJobTitle").text.strip()
    except NoSuchElementException:
        job_title = ""
    
    try:
        company_name = driver.find_element(By.CSS_SELECTOR,
            "#view_inner > div > div.toggle-head.z-hi.u-sticky-m.bg-inverse.bb-m > div.row.is-m.no-wrap.v-align-center.p10t > div > b > a"
        ).text.strip()
    except NoSuchElementException:
        company_name = ""
    
    publication_date = ""
    # Première méthode : via un sélecteur CSS spécifique pour la date (à adapter selon votre page)
    try:
        publication_date = driver.find_element(By.CSS_SELECTOR, "span.job-publication-date").text.strip()
    except NoSuchElementException:
        # Deuxième méthode : extraction depuis le contenu textuel complet
        try:
            full_text = driver.find_element(By.CSS_SELECTOR, "div.u-scrolly.t-small").text
            # On cherche le texte associé à "Publiée le:" ou "Date de publication" (adaptable selon le site)
            date_match = re.search(r"(Publi[eé]e? le|Date de publication)[:\s]*(.+)", full_text, re.IGNORECASE)
            if date_match:
                publication_date = date_match.group(2).strip()
        except NoSuchElementException:
            publication_date = ""
    
    try:
        full_details = driver.find_element(By.CSS_SELECTOR, "div.u-scrolly.t-small").text.strip()
        segmented_details = text_segmentation(full_details)
    except NoSuchElementException:
        segmented_details = {}

    offer = {
        "job_title": job_title,
        "company_name": company_name,
        "date_publication": publication_date
    }
    offer.update(segmented_details)
    offers.append(offer)
    return offers

def find_number_of_pages(driver: webdriver.Chrome):
    """
    Tente de déterminer le nombre total de pages dans la pagination des offres.
    """
    try:
        num_of_pages_el = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul.pagination li.pagination-last-d a"))
        )
        num_of_pages = num_of_pages_el.get_attribute("href").split("page=")[1]
        print("Number of pages found:", num_of_pages)
        return int(num_of_pages)
    except TimeoutException:
        print("Couldn't find number of pages.")
        return 1

def change_page(driver: webdriver.Chrome, num_pages: int):
    """
    Change de page en incrémentant le paramètre 'page' de l'URL.
    Retourne True si la page suivante existe et a été chargée, sinon False.
    """
    try:
        next_page = int(driver.current_url.split("?page=")[1]) + 1
    except (IndexError, ValueError):
        next_page = 1
    if next_page <= num_pages:
        try:
            base_url = driver.current_url.split("?page=")[0]
            next_url = base_url + "?page=" + str(next_page)
            driver.get(next_url)
            WebDriverWait(driver, 10).until(EC.url_to_be(next_url))
            return True
        except TimeoutException:
            print("No more pages to load (timeout).")
            return False
    else:
        print("No more pages to load.")
        return False

def access_job_offer(driver: webdriver.Chrome):
    """
    Localise toutes les offres d'emploi sur la page et clique sur chacune pour extraire les détails.
    Après extraction, ferme le volet de détail pour revenir à la liste.
    """
    job_offers = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.media-list.in-card > li.has-pointer-d"))
    )
    offers = []
    current_url = driver.current_url
    print("Found {} job offers on the page.".format(len(job_offers)))
    for i in range(len(job_offers)):
        try:
            job_offers = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.media-list.in-card > li.has-pointer-d"))
            )
            job_offer = job_offers[i]
            driver.execute_script("arguments[0].scrollIntoView();", job_offer)
            time.sleep(0.5)
            job_offer.click()
            offers = extract_job_details(driver, offers)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.icon.is-times.has-pointer.t-mute.m0"))
            ).click()
        except (ElementClickInterceptedException, ElementNotInteractableException, NoSuchElementException) as e:
            print("Error while processing a job offer:", e)
            if driver.current_url != current_url:
                driver.get(current_url)
                WebDriverWait(driver, 5).until(EC.url_to_be(current_url))
            continue
    return offers

def save_json(data, filename="offres_emploi_bayt.json"):
    """
    Sauvegarde les données dans un fichier JSON avec indentation.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Extraction saved in {filename}.")

def extract_job_offers(driver: webdriver.Chrome):
    """
    Fonction principale pour extraire toutes les offres d'emploi depuis Bayt.
    La recherche initiale est lancée via access_bayt, puis les pages sont parcourues.
    """
    data = []
    access_bayt(driver)
    num_pages = find_number_of_pages(driver)
    current_page = 1
    
    while True:
        print("Extracting job offers from page {}: {}".format(current_page, driver.current_url))
        page_offers = access_job_offer(driver)
        data.extend(page_offers)
        current_page += 1
        if not change_page(driver, num_pages):
            break
        time.sleep(1)
    
    print("Finished processing all pages. Total offers extracted: ", len(data))
    save_json(data, filename="offres_emploi_bayt.json")
    print("Extraction terminée!")
    
if __name__ == "__main__":
    driver = init_driver()
    extract_job_offers(driver)
    driver.quit()
