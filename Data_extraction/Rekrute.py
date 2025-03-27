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
driver=init_driver()
driver.get("https://www.rekrute.com")
time.sleep(10)