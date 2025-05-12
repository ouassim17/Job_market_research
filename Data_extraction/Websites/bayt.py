import datetime
import re
import time
from jsonschema import ValidationError
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium_init import (
    check_duplicate,
    init_driver,
    save_json,
    setup_logger,
    validate_json,
)
import undetected_chromedriver as uc

logger = setup_logger("bayt.log")

    

def access_bayt(driver: webdriver.Chrome):
    base_url = "https://www.bayt.com/en/morocco/"
    driver.get(base_url)

    # Wait for search bar, then enter query
    search_input = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input#text_search"))
    )
    search_input.clear()
    while driver.current_url == base_url:
        search_input.send_keys("DATA" + Keys.RETURN)

def extract_job_info(driver: webdriver.Chrome):
    try:
        data = []
    except FileNotFoundError:
        data = []

    job_urls = WebDriverWait(driver, 5).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "div.row.is-compact.is-m.no-wrap > h2 > a")
        )
    )
    job_urls = [job_url.get_attribute("href") for job_url in job_urls]
    offers = []

    logger.info(f"Found {len(job_urls)} job offers.")
    for job_url in job_urls:
        try:
            if check_duplicate(data, job_url):
                continue

            driver.get(job_url)

            try:
                pop_up = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "body > div.cky-consent-container > div > button > img")
                    )
                )
                pop_up.click()
            except (ElementClickInterceptedException, ElementNotInteractableException):
                logger.info("No popup found — continuing.")

            offer = extract_job_details(driver)
            offer["job_url"] = job_url

            try:
                validate_json(offer)
                offers.append(offer)
            except ValidationError as e:
                logger.exception(f"Erreur lors de validation JSON : {e}")
                continue

        except Exception:
            logger.exception("An error occurred while extracting job details")

    return offers

def extract_job_details(driver: webdriver.Chrome):
    try:
        titre = driver.find_element(By.CSS_SELECTOR, 'h1[id="job_title"]').text.strip()
    except NoSuchElementException:
        titre = ""

    try:
        publication_date = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span[id="jb-posted-date"]'))
        ).text
    except NoSuchElementException:
        publication_date = ""

    try:
        companie = driver.find_element(By.CSS_SELECTOR, 'a[class="t-default t-bold"]>span').text.strip()
    except NoSuchElementException:
        companie = ""

    try:
        job_details = driver.find_element(By.CSS_SELECTOR, 'div[class="t-break"]').text.strip()
    except NoSuchElementException:
        job_details = ""

    offer = {
        "titre": titre,
        "publication_date": publication_date,
        "companie": companie,
        "via": "Bayt",
    }

    return offer

def main():
    start_time = time.time()
    logger.info("Début de l'extraction des offres d'emploi sur Bayt.com")

    try:
        driver = init_driver()
        if not driver:
            logger.error("WebDriver failed to start. Exiting program.")
            return

        data = []
        access_bayt(driver)

        # Extract job offers
        data.extend(extract_job_info(driver))

    except Exception as e:
        logger.exception(f"An error occurred during extraction: {e}")

    finally:
        if driver:
            driver.quit()
        save_json(data, filename="offres_emploi_bayt.json")
        logger.info(f"Extraction terminée en {time.time() - start_time} secondes.")

    return data

main()