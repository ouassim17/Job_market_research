import json
import os
import time

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from Data_extraction.Websites.selenium_init import init_driver, save_json, setup_logger

logger = setup_logger("emploi.log")

# Chargement des offres déjà collectées
output_filename = "offres_emploi_emploi.json"
if os.path.exists(output_filename):
    with open(output_filename, "r", encoding="utf-8") as f:
        existing_jobs = json.load(f)
else:
    existing_jobs = []


# Création d'un ensemble des dates de publication déjà présentes
existing_publication_dates = {
    job["publication_date"]
    for job in existing_jobs
    if "publication_date" in job and job["publication_date"]
}

# Initialisation du driver
driver = init_driver()

# Liste pour stocker les nouvelles données scrappées
new_jobs = []
# Ensemble pour suivre les URLs déjà collectées dans cette session (pour éviter les doublons sur cette séance)
collected_urls = set()


def access_emploi(driver: webdriver.Chrome):
    # Accès à l'URL initiale pour soumettre la recherche "DATA AI ML"
    driver.get(
        "https://www.emploi.ma/recherche-jobs-maroc/data?f%5B0%5D=im_field_offre_metiers%3A31"
    )
    try:
        search_input = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input#keywordSearch"))
        )
        search_input.clear()
        search_input.send_keys("DATA AI ML")
        search_input.send_keys(Keys.RETURN)
    except TimeoutException:
        logger.exception(
            "Champ de recherche introuvable, la requête ne sera pas envoyée."
        )


def get_number_pages(driver: webdriver.Chrome):
    pages = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "pager-item active pagination-numbers")
        )
    )
    max_pages = int(pages[-1].find_element(By.CSS_SELECTOR, "a").text.strip())
    return max_pages


try:
    access_emploi(driver)
    max_pages = get_number_pages(driver)
    # Boucle de pagination
    page = 1
    while page <= max_pages:
        # Construction de l'URL pour la page courante avec le paramètre de pagination
        url = f"https://www.emploi.ma/recherche-jobs-maroc/Data?page={page}&f%5B0%5D=im_field_offre_metiers%3A31"
        logger.info(f"Scraping de la page {page} : {url}")
        driver.get(url)

        # Attendre que les cartes d'offres soient chargées
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "div.card.card-job")
                )
            )
        except TimeoutException:
            logger.exception(
                f"Aucune carte trouvée sur la page {page} ou temps d'attente dépassé."
            )
            break

        cards = WebDriverWait(driver, 3).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.card.card-job"))
        )
        logger.info(f"Nombre de cartes trouvées sur la page {page} : {len(cards)}")

        # Si aucune carte n'est présente, sortir de la boucle
        if not cards:
            logger.warning("Aucune offre trouvée sur cette page, fin de la pagination.")
            break

        for index, card in enumerate(cards, start=1):
            # Récupérer l'URL de l'offre
            try:
                job_url = (
                    card.get_attribute("data-href").strip()
                    if card.get_attribute("data-href")
                    else ""
                )
            except Exception as e:
                logger.exception(
                    f"[Carte {index} - page {page}] Erreur lors de la récupération de l'URL : {e}"
                )
                job_url = ""

            # Filtre de doublon basé sur l'URL déjà collectée durant cette session
            if job_url and job_url in collected_urls:
                logger.warning(
                    f"[Carte {index} - page {page}] Offre déjà collectée (même URL)."
                )
                continue

            # Récupérer le titre de l'offre
            try:
                title = card.find_element(By.CSS_SELECTOR, "h3 a").text.strip()
            except NoSuchElementException:
                logger.exception(f"[Carte {index} - page {page}] Titre non trouvé.")
                title = ""

            # Récupérer le nom de l'entreprise
            try:
                company = card.find_element(
                    By.CSS_SELECTOR, "a.card-job-company"
                ).text.strip()
            except NoSuchElementException:
                logger.exception(
                    f"[Carte {index} - page {page}] Nom de l'entreprise non trouvé."
                )
                company = ""

            # Récupérer la description
            try:
                description = card.find_element(
                    By.CSS_SELECTOR, "div.card-job-description p"
                ).text.strip()
            except NoSuchElementException:
                logger.exception(
                    f"[Carte {index} - page {page}] Description non trouvée."
                )
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
                    if (
                        "Niveau d´études requis" in txt
                        or "Niveau d’études requis" in txt
                    ):
                        try:
                            niveau_etudes = li.find_element(
                                By.TAG_NAME, "strong"
                            ).text.strip()
                        except NoSuchElementException:
                            niveau_etudes = ""
                    elif "Niveau d'expérience" in txt:
                        try:
                            niveau_experience = li.find_element(
                                By.TAG_NAME, "strong"
                            ).text.strip()
                        except NoSuchElementException:
                            niveau_experience = ""
                    elif "Contrat proposé" in txt:
                        try:
                            contrat = li.find_element(
                                By.TAG_NAME, "strong"
                            ).text.strip()
                        except NoSuchElementException:
                            contrat = ""
                    elif "Région de" in txt:
                        try:
                            region = li.find_element(By.TAG_NAME, "strong").text.strip()
                        except NoSuchElementException:
                            region = ""
                    elif "Compétences clés" in txt:
                        try:
                            competences = li.find_element(
                                By.TAG_NAME, "strong"
                            ).text.strip()
                        except NoSuchElementException:
                            competences = ""
            except NoSuchElementException:
                logger.exception(
                    f"[Carte {index} - page {page}] Section des détails complémentaires non trouvée."
                )

            # Récupérer la date de publication
            try:
                pub_date = (
                    card.find_element(By.CSS_SELECTOR, "time")
                    .get_attribute("datetime")
                    .strip()
                )
            except NoSuchElementException:
                logger.exception(
                    f"[Carte {index} - page {page}] Date de publication non trouvée."
                )
                pub_date = ""

            # Vérification : si la date de publication existe déjà dans le fichier, considérer l'offre comme doublon
            if pub_date and pub_date in existing_publication_dates:
                logger.info(
                    f"[Carte {index} - page {page}] Offre déjà existante (date de publication identique: {pub_date})."
                )
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
                "publication_date": pub_date,
            }

            # Ajout de l'offre aux nouvelles offres et mémorisation de l'URL
            new_jobs.append(job)
            if job_url:
                collected_urls.add(job_url)
            # On ajoute également la date dans l'ensemble pour éviter qu'un job ultérieur de même date soit ré-ajouté
            if pub_date:
                existing_publication_dates.add(pub_date)

        # Passage à la page suivante
        page += 1
        # Pause courte entre chaque page pour éviter de surcharger le serveur
        time.sleep(1)

except Exception as e:
    logger.exception("Erreur lors du scraping :", e)
finally:
    driver.quit()
    logger.info("Extraction terminée !")
    all_jobs = existing_jobs + new_jobs
    save_json(all_jobs, "offres_emploi_emploi.json")

logger.info("Nombre total d'offres nouvellement extraites :", len(new_jobs))
