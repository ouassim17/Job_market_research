from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os
import time

current_path = os.path.abspath(__file__)
current_dir= os.path.dirname(current_path)


def init_driver(executable_path=os.path.dirname(current_dir) + "\chromedriver-win64\chromedriver.exe"):
    # Creation et configuration du Driver, pour pointer sur le driver changez le chemin executable_path 
    print("The executable path is: ", executable_path)
    service = Service(executable_path)
    chrome_options = Options()
   
    driver = webdriver.Chrome(options=chrome_options, service=service)
    driver.implicitly_wait(2)  # Time before the program exits in case of exception in seconds, will not wait if the program runs normally
    
    return driver
def enter_search(driver, search):
    key_word=WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR,'input#keyword')))
    key_word.send_keys(search)
    search_button=WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR,'button#SearchHpBouton')))
    search_button.click()
    
driver=init_driver()
driver.get("https://www.rekrute.com")
enter_search(driver, "Data")

time.sleep(10)