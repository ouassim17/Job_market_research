import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

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
def highlight(element, effect_time=0.3, color="yellow", border="2px solid red",active=True):
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