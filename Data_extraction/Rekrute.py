import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium_init import *
import time


# --- Fonction d'extraction des offres sur la page courante ---
def extract_offers():
    offers_list = []
    
    holders = driver.find_elements(By.CSS_SELECTOR, "div.holder")
    print("Offres trouvées sur cette page :", len(holders)-1)
    
    for holder in holders[1:]:  # Ignorer le premier conteneur qui est un filtre
        try:
            info_divs = holder.find_elements(By.CSS_SELECTOR, "div.info")
        except NoSuchElementException:
            info_divs = []

        Job_title = ""
        try:
            parent_div = holder.find_element(By.XPATH, './ancestor::div[1]')

            job_title = parent_div.find_element(By.CSS_SELECTOR, 'a.titreJob')
            job_url= job_title.get_attribute("href")
            highlight(job_title)
            Job_title = job_title.text.strip()

        # 1. Récupérer les prerequis du poste 
        except NoSuchElementException:
            Job_title = ""

        required_skills = ""
        if len(info_divs) >= 1:
            try:
                field = holder.find_element(By.CSS_SELECTOR, 'i.fa.fa-search')
                highlight(field)
                parent_div = field.find_element(By.XPATH, './ancestor::div[1]')
                highlight(parent_div)
                required_skills = parent_div.find_element(By.TAG_NAME, "span").text.strip()
            except NoSuchElementException:
                required_skills = ""
        # 2. Récupérer la description de la societe
        comp_desc = ""
        if len(info_divs) >= 2:
            try:
                field = holder.find_element(By.CSS_SELECTOR, 'i.fa.fa-industry')
                highlight(field)
                parent_div = field.find_element(By.XPATH, './ancestor::div[1]')
                highlight(parent_div)
                comp_desc = parent_div.find_element(By.TAG_NAME, "span").text.strip()
                
            except NoSuchElementException:
                comp_desc = ""
        
        
        # 3. Récupérer la description de la mission
        mission = ""
        if len(info_divs) >= 2:
            try:
                field = holder.find_element(By.CSS_SELECTOR, 'i.fa.fa-binoculars')
                highlight(field)
                parent_div = field.find_element(By.XPATH, './ancestor::div[1]')
                highlight(parent_div)
                mission = parent_div.find_element(By.TAG_NAME, "span").text.strip()
            except NoSuchElementException:
                mission = ""
        # 4. Récupérer les dates de publication et le nombre de postes (<em class="date">)
        pub_start = pub_end = postes = ""
        try:
            date_elem = holder.find_element(By.CSS_SELECTOR, "em.date")
            highlight(date_elem)
            spans = date_elem.find_elements(By.TAG_NAME, "span")
            pub_start = spans[0].text.strip() if len(spans) > 0 else ""
            pub_end = spans[1].text.strip() if len(spans) > 1 else ""
            postes = spans[2].text.strip() if len(spans) > 2 else ""
        except NoSuchElementException:
            pass
        
        # 5. Récupérer les détails complémentaires (dernière div.info contenant une liste <li>)
        secteur = fonction = experience = niveau = contrat = ""
        if len(info_divs) >= 3:
            try:
                details_div = info_divs[-1]
                li_items = details_div.find_elements(By.TAG_NAME, "li")
                for li in li_items:
                    highlight(li)
                    txt = li.text.strip()
                    if "Secteur d'activité" in txt:
                        secteur = txt.split(":", 1)[1].strip()
                    elif "Fonction" in txt:
                        fonction = txt.split(":", 1)[1].strip()
                    elif "Expérience requise" in txt:
                        experience = txt.split(":", 1)[1].strip()
                    elif "Niveau d'étude demandé" in txt:
                        niveau = txt.split(":", 1)[1].strip()
                    elif "Type de contrat proposé" in txt:
                        contrat = txt.split(":", 1)[1].strip()
            except Exception:
                pass
        
        offer = {
            "job_title": Job_title,
            "required_skills": required_skills,
            "company_description": comp_desc,
            "mission": mission,
            "publication_start": pub_start,
            "publication_end": pub_end,
            "postes": postes,
            "secteur": secteur,
            "fonction": fonction,
            "experience": experience,
            "niveau": niveau,
            "type_contrat": contrat,
            "url": job_url
        }
        offers_list.append(offer)
    return offers_list
def save_json(data, filename="offres_emploi.json"):
    # --- Sauvegarde locale en JSON (pour vérification) ---
    json_filename = filename
    with open(json_filename, "w", encoding="utf-8") as js_file:
        json.dump(data, js_file, ensure_ascii=False, indent=4)
    print(f"Les informations ont été enregistrées dans {json_filename}.")
def access_rekrute(driver):
    
    # Accéder à la page de base
    base_url = "https://www.rekrute.com/offres-emploi-maroc.html"
    driver.get(base_url)
    
    # Attendre que la barre de recherche soit disponible, puis saisir "DATA"
    search_input = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#keywordSearch"))
    )
    search_input.clear()
    search_input.send_keys("DATA" + Keys.RETURN)
    
def get_pages_url(driver):
    try:
        # Sélecteur adapté pour la nouvelle structure
        pagination = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.slide-block div.pagination"))
        )
        amount_of_offers=pagination.find_element(By.CSS_SELECTOR, "ul.amount").find_elements(By.TAG_NAME, "li")
        last_page_amount=amount_of_offers[-1]
        page_link=last_page_amount.find_element(By.TAG_NAME,"a").get_attribute("href")
        driver.get(page_link)
        time.sleep(2)

        pagination = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.slide-block div.pagination select"))
        )
        page_options = pagination.find_elements(By.TAG_NAME, "option")
        total_pages = len(page_options)
        print("Nombre total de pages :", total_pages)
        page_urls = [url.get_attribute("value") for url in page_options]

    except Exception as e:
        print("Pagination select non trouvée. Utilisation d'une seule page.", e)
        page_urls = []
    return page_urls
def change_page(driver, page_urls,data):
    if page_urls:
        # On ignore la première option puisque c'est la page actuelle déjà traitée
        for url in page_urls:
            
            # Si l'URL est relative, on complète avec le domaine
            if not url.startswith("http"):
                url = "https://www.rekrute.com" + url
                print("accessing the url: ", url)
            print("Navigation vers la page :", url)
            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
            )
            data.extend(extract_offers())
            print("Page traitée, total offres cumulées :", len(data))
try:
    # --- Initialisation du driver Chrome ---
    driver = init_driver()
    data = []  # Liste qui contiendra toutes les offres
    access_rekrute(driver)
    print("Accès à la page de recherche réussi.")
    page_urls=get_pages_url(driver)
    print("les urls extraits: ",page_urls)
   
    print("Extraction des offres sur la page actuelle...")
    change_page(driver,page_urls,data)
    
    save_json(data, filename="offres_emploi_rekrute.json")
    
            
except Exception as e:
    print("Erreur lors de l'extraction :", e)
finally:
    driver.quit()
    print("Extraction terminée !")



