import re
from selenium import webdriver

# Define parse_details_text function
def parse_details_text(details_text):
    """Parse the details text and return a dictionary of extracted details."""
    # Example parsing logic (adjust as needed for your use case)
    details = {}
    lines = details_text.split("\n")
    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            details[key.strip()] = value.strip()
    return details
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium_init import (
    check_duplicate,
    init_driver,
    load_json,
    save_json,
    setup_logger,
    validate_json,
)
import undetected_chromedriver as uc

logger = setup_logger("maroc_ann.log")

def extract_offers(driver: webdriver.Chrome):
    """ Extract job listings from the current page. """
    offers_list = []
    try:
        holders = driver.find_elements(By.CSS_SELECTOR, "li:not(.adslistingpos) div.holder")
        logger.info(f"Offers found on this page: {len(holders)}")
    except (NoSuchElementException, TimeoutException) as e:
        logger.warning(f"Error extracting offers: {e}")

    for holder in holders:
        try:
            a_tag = holder.find_element(By.XPATH, "./..")
            job_url = a_tag.get_attribute("href")
            job_title = holder.find_element(By.TAG_NAME, "h3").text.strip()
            location = holder.find_element(By.CLASS_NAME, "location").text.strip()
            offer = {"titre": job_title, "region": location, "job_url": job_url}
            offers_list.append(offer)
        except NoSuchElementException as e:
            logger.exception(f"Element missing in offer extraction: {e}")
            continue

        parsed_details = parse_details_text(details_text)  # Ensure parse_details_text is defined or imported

def extract_offer_details(driver, offer_url):
    """ Extract job details from an individual listing. """
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
        logger.exception(f"Timeout extracting details for {offer_url}")
    except WebDriverException as we:
        logger.exception(f"WebDriverException for {offer_url}: {we}")
    except Exception as e:
        logger.exception(f"Error extracting details for {offer_url}: {e}")

    return details

def change_page(driver: webdriver.Chrome, base_url, page_num):
    url = base_url.format(page_num)
    logger.info(f"Scraping page {page_num}")
    try:
        driver.get(url)
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
        )
        return True
    except TimeoutException as e:
        logger.exception(f"Error loading page {page_num}: {e}")
        return False

def main():
    driver = init_driver()
    if not driver:
        logger.error("Failed to start WebDriver. Exiting program.")
        return

    old_data = load_json("offres_marocannonces.json")
    new_offres = []
    new_data = []

    try:
        base_url = "https://www.marocannonces.com/maroc/offres-emploi-b309.html?kw=data+&pge={}"
        page_num = 1
        while change_page(driver, base_url, page_num):
            offers_list = extract_offers(driver)
            if not offers_list:
                logger.info("No offers found on this page. Ending pagination.")
                break

            new_offres.extend(offers_list)
            page_num += 1

        logger.info(f"Total extracted offers (before details): {len(new_offres)}")

        for offer in new_offres:
            offer_url = offer.get("job_url")
            if not offer_url:
                logger.info("Missing URL for offer, skipping.")
                continue

            if check_duplicate(old_data, offer_url):
                continue

            logger.info(f"Extracting details for offer: {offer_url}")
            details = extract_offer_details(driver, offer_url)
            offer.update(details)

            try:
                pub_date = offer.get("publication_date")
                existing_pub_dates = [date.get("publication_date") for date in old_data]
                if pub_date and pub_date in existing_pub_dates:
                    logger.info(f"Existing offer detected (date: {pub_date}), skipping.")
                    continue
            except Exception as e:
                logger.warning(f"Error extracting publication date: {e}")
                continue

            try:
                validate_json(offer)
                new_data.append(offer)
            except Exception as e:
                logger.exception(f"JSON validation error for {offer_url}: {e}")
                continue

    except Exception as e:
        logger.warning(f"Error during extraction: {e}")

    finally:
        if driver:
            driver.quit()
        logger.info(f"New offers collected: {len(new_data)}")
        save_json(new_data, "offres_marocannonces.json")
        logger.info("Extraction complete!")

    return new_data

main()