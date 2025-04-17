import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import json

from jsonschema import validate, ValidationError
current_path = os.path.abspath(__file__)
current_dir= os.path.dirname(current_path)


def init_driver(executable_path=os.path.dirname(current_dir) + "\chromedriver-win64\chromedriver.exe"):
    # Creation et configuration du Driver, pour pointer sur le driver changez le chemin executable_path 
    print("The executable path is: ", executable_path)
    service = Service(executable_path)
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    #chrome_options.add_argument("--headless")

   
    driver = webdriver.Chrome(options=chrome_options, service=service)
    driver.implicitly_wait(2)  # Time before the program exits in case of exception in seconds, will not wait if the program runs normally
    
    return driver
def highlight(element, effect_time=0.1, color="yellow", border="2px solid red",active=True):
    if active:
        """Highlights (blinks) a Selenium WebDriver element."""
        driver = element._parent  
        original_style = element.get_attribute("style")
        highlight_style = f"background: {color}; border: {border};"

        driver.execute_script(
            f"arguments[0].setAttribute('style', arguments[1]);", element, highlight_style
        )
        import time
        time.sleep(effect_time)
        driver.execute_script(
            f"arguments[0].setAttribute('style', arguments[1]);", element, original_style
        )
def save_json(data:list, filename="default.json"):
    # --- Sauvegarde locale en JSON (pour vérification) ---
    existing_data = []
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as js_file:
                existing_data = json.load(js_file)
    except FileNotFoundError as e:
        print(f"Error finding file, creating new one")
        json.dump
    with open(filename, "w", encoding="utf-8") as js_file:
        
        merged_data = existing_data + data
        print(f'Saving {len(merged_data)} jobs to {filename}, {len(data)} new jobs')
        json.dump(merged_data, js_file, ensure_ascii=False, indent=4)
    
def validate_json(data, schema_path=os.path.join(current_dir, "Job_schema.json")):
    with open(schema_path) as f:
        schema = json.load(f)
    try:
        validate(data, schema)
    except ValidationError as e:
        print("Invalid JSON:", e.message)
def check_duplicate(data, job_url):
    # Check if the job URL already exists in the data
    for job in data[:][:]:
        if job.get("job_url") == job_url:
            print(f"Duplicate found: {job_url}")
            return True
    return False
