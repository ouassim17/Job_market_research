import os
import undetected_chromedriver as uc
#from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
import json
import logging
import time
from jsonschema import validate, ValidationError
current_path = os.path.abspath(__file__)
current_dir= os.path.dirname(current_path)


def init_driver(executable_path=os.path.dirname(current_dir) + "\chromedriver-win64\chromedriver.exe",proxy_index=0):
    # Creation du proxy
    proxy_path=current_dir+"\checked_proxies.txt"
    proxies=open(proxy_path,"r").readlines()
    proxy_ip_port=str(proxies[proxy_index]).strip()
    # Creation et configuration du Driver, pour pointer sur le driver changez le chemin executable_path 
    service = Service(executable_path)
    chrome_options = Options()
    #chrome_options.add_argument(f"--proxy-server={proxy_ip_port}")
    chrome_options.add_argument("--start-maximized")
    driver = uc.Chrome(options=chrome_options, service=service)
    #chrome_options.add_argument("--headless")
    driver.implicitly_wait(2)  # Time before the program exits in case of exception in seconds, will not wait if the program runs normally
    
    return driver

def highlight(element, effect_time=0.1, color="yellow", border="2px solid red", active=True):
    if active:
        driver = element._parent  
        original_style = element.get_attribute("style")

        # Inject pulse animation CSS into the page
        driver.execute_script("""
            if (!document.getElementById('pulse-style')) {
                const style = document.createElement('style');
                style.id = 'pulse-style';
                style.innerHTML = `
                    @keyframes pulse {
                        0% {
                            box-shadow: 0 0 0 0 rgba(255, 0, 0, 0.7);
                        }
                        70% {
                            box-shadow: 0 0 0 10px rgba(255, 0, 0, 0);
                        }
                        100% {
                            box-shadow: 0 0 0 0 rgba(255, 0, 0, 0);
                        }
                    }
                `;
                document.head.appendChild(style);
            }
        """)

        # Apply highlight + pulse animation
        highlight_style = f"background: {color}; border: {border}; animation: pulse 1s infinite;"
        driver.execute_script(
            "arguments[0].setAttribute('style', arguments[1]);", element, highlight_style
        )

        import time
        time.sleep(effect_time)

        # Scroll smoothly to center
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)

        # Remove animation and restore original style
        driver.execute_script(
            "arguments[0].setAttribute('style', arguments[1]);", element, original_style
        )

        
def save_json(data:list, filename="default.json"):
    # --- Sauvegarde locale en JSON (pour vérification) ---
    existing_data = []
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as js_file:
                existing_data = json.load(js_file)
    except FileNotFoundError:
        logging.error(f"File not found, creating new one")
        json.dump
    with open(filename, "w", encoding="utf-8") as js_file:
        
        merged_data = existing_data + data
        logging.info(f'Saving {len(merged_data)} jobs to {filename}, {len(data)} new jobs')
        json.dump(merged_data, js_file, ensure_ascii=False, indent=4)
    
def validate_json(data, schema_path=os.path.join(current_dir, "Job_schema.json")):
    with open(schema_path) as f:
        schema = json.load(f)
    try:
        validate(data, schema)
    except ValidationError as e:
        logging.error(f"Validation error: {e.message}")
        return e
        

def check_duplicate(data, job_url):
    # Check if the job URL already exists in the data
    for job in data[:][:]:
        if job.get("job_url") == job_url:
            logging.warning(f"Duplicate found: {job_url}")
            return True
    return False

# Set up a logger
def setup_logger(filename="app.log"):
    logger = logging.getLogger('my_logger')
    logger.propagate = False  # Disable propagation to root logger

    if not logger.hasHandlers():  # Avoid adding handlers multiple times
        # Set the default logging configuration
        file_handler = logging.FileHandler(filename) #Log to a file
        console_handler = logging.StreamHandler()  # Log to the console
        # Set logging level
        file_handler.setLevel(logging.INFO)
        console_handler.setLevel(logging.INFO)
        # Set the time format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)
        
    return logger

