import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium_init import init_driver, highlight
import time

def access_bayt(driver):
    
    # Accéder à la page de base
    base_url = "https://www.bayt.com/en/morocco/"
    driver.get(base_url)
    
    # Attendre que la barre de recherche soit disponible, puis saisir "DATA"
    search_input = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input#text_search"))
    )
    search_input.clear()
    search_input.send_keys("DATA" + Keys.RETURN)
def 