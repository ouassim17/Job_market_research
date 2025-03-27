from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

def init_driver(executable_path=r'C:\Users\hp\Documents\Code projects\Stage 2024\Web scrapping\chromedriver\win64-125.0.6422.142\chromedriver-win64\chromedriver.exe'):
    # Creation et configuration du Driver, pour pointer sur le driver changez le chemin executable_path 
    service = Service(executable_path)
    chrome_options = Options()
   
    driver = webdriver.Chrome(options=chrome_options, service=service)
    driver.implicitly_wait(2)  # Time before the program exits in case of exception in seconds, will not wait if the program runs normally
    
    
    driver.execute_cdp_cmd('Network.setBlockedURLs', {"urls": ["*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp", "*.bmp"]})
    driver.execute_cdp_cmd('Network.enable', {})
    return driver