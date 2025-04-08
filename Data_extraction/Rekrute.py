import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


# --- Fonction d'extraction des offres sur la page courante ---
def extract_offers():
    offers_list = []
    holders = driver.find_elements(By.CSS_SELECTOR, "div.holder")
    print("Offres trouvées sur cette page :", len(holders))
    
    for holder in holders[1:]:  # Ignorer le premier conteneur qui est un filtre
        try:
            info_divs = holder.find_elements(By.CSS_SELECTOR, "div.info")
        except NoSuchElementException:
            info_divs = []
        
        # 1. Récupérer la description (première div.info)
        comp_desc = ""
        if len(info_divs) >= 1:
            try:
                comp_desc = info_divs[0].find_element(By.TAG_NAME, "span").text.strip()
            except NoSuchElementException:
                comp_desc = ""
        
        # 2. Récupérer la mission (deuxième div.info)
        mission = ""
        if len(info_divs) >= 2:
            try:
                mission = info_divs[1].find_element(By.TAG_NAME, "span").text.strip()
            except NoSuchElementException:
                mission = ""
        
        # 3. Récupérer les dates de publication et le nombre de postes (<em class="date">)
        pub_start = pub_end = postes = ""
        try:
            date_elem = holder.find_element(By.CSS_SELECTOR, "em.date")
            spans = date_elem.find_elements(By.TAG_NAME, "span")
            pub_start = spans[0].text.strip() if len(spans) > 0 else ""
            pub_end = spans[1].text.strip() if len(spans) > 1 else ""
            postes = spans[2].text.strip() if len(spans) > 2 else ""
        except NoSuchElementException:
            pass
        
        # 4. Récupérer les détails complémentaires (dernière div.info contenant une liste <li>)
        secteur = fonction = experience = niveau = contrat = ""
        if len(info_divs) >= 3:
            try:
                details_div = info_divs[-1]
                li_items = details_div.find_elements(By.TAG_NAME, "li")
                for li in li_items:
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
            "company_description": comp_desc,
            "mission": mission,
            "publication_start": pub_start,
            "publication_end": pub_end,
            "postes": postes,
            "secteur": secteur,
            "fonction": fonction,
            "experience": experience,
            "niveau": niveau,
            "type_contrat": contrat
        }
        offers_list.append(offer)
    return offers_list

# --- Initialisation du driver Chrome ---
driver = webdriver.Chrome()
data = []  # Liste qui contiendra toutes les offres

try:
    # Accéder à la page de base
    base_url = "https://www.rekrute.com/offres-emploi-maroc.html"
    driver.get(base_url)
    
    # Attendre que la barre de recherche soit disponible, puis saisir "DATA"
    search_input = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#keywordSearch"))
    )
    search_input.clear()
    search_input.send_keys("DATA" + Keys.RETURN)
    
    # Attendre que les résultats s'affichent (présence d'au moins un conteneur "div.holder")
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
    )
    
    # --- Extraction des offres sur la première page ---
    data.extend(extract_offers())
    
    # --- Récupération de la pagination via le <select> dans la div "slide-block" ---
    try:
        # Sélecteur adapté pour la nouvelle structure
        pagination_select = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.slide-block div.pagination select"))
        )
        options = pagination_select.find_elements(By.TAG_NAME, "option")
        total_pages = len(options)
        print("Nombre total de pages (d'après le select) :", total_pages)
    except Exception as e:
        print("Pagination select non trouvée. Utilisation d'une seule page.", e)
        options = []
    
    # --- Itération sur les pages suivantes en utilisant les options du <select> ---
    if options:
        # On ignore la première option puisque c'est la page actuelle déjà traitée
        for option in options[1:]:
            page_url = option.get_attribute("value")
            # Si l'URL est relative, on complète avec le domaine
            if not page_url.startswith("http"):
                page_url = "https://www.rekrute.com" + page_url
            print("Navigation vers la page :", page_url)
            driver.get(page_url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
            )
            data.extend(extract_offers())
            print("Page traitée, total offres cumulées :", len(data))
            
except Exception as e:
    print("Erreur lors de l'extraction :", e)
finally:
    driver.quit()
    print("Extraction terminée !")

# --- Sauvegarde locale en JSON (pour vérification) ---
json_filename = "offres_emploi.json"
with open(json_filename, "w", encoding="utf-8") as js_file:
    json.dump(data, js_file, ensure_ascii=False, indent=4)
print(f"Les informations ont été enregistrées dans {json_filename}.")


