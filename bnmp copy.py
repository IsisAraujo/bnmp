import os
import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import time
import random

OUTPUT_DIR = 'output'
OUTPUT_FILE = 'output/1.dados_gerais.json'
EXCEL_FILE = 'output/2.dados_gerais.xlsx'
BNMP_URL = 'https://portalbnmp.cnj.jus.br/bnmpportal/api/pesquisa-pecas/filter'
MAX_ITEMS_PER_PAGE = 30
RENEW_REQUEST_THRESHOLD = 40

HEADERS = {
    'User-Agent': (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    )
}

class ReCaptchaBypasser:
    def __init__(self, chrome_driver_path, chrome_extension_path):
        self.chrome_driver_path = chrome_driver_path
        self.chrome_extension_path = chrome_extension_path
        self.driver = None

    def initialize_driver(self):
        # Chrome options
        chrome_options = Options()
        chrome_options.add_extension(self.chrome_extension_path)

        # Service object
        service = Service(self.chrome_driver_path)

        # Initializing the driver with service object
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def bypass_recaptcha(self, page_url):
        self.driver.get(page_url)
        time.sleep(2)

        try:
            iframe = self.driver.find_element(By.XPATH, '//iframe[@title="reCAPTCHA"]')
            self.driver.switch_to.frame(iframe)

            checkbox = self.driver.find_element(By.CLASS_NAME, 'recaptcha-checkbox-border')
            checkbox.click()
            self.driver.implicitly_wait(5)
        except NoSuchElementException:
            self.driver.quit()
            exit()

        self.driver.switch_to.default_content()

        try:
            challenge_iframe = self.driver.find_element(By.XPATH, '//iframe[@title="recaptcha challenge expires in two minutes"]')
            self.driver.switch_to.frame(challenge_iframe)

            buster_button = self.driver.find_element(By.XPATH, '//*[@id="rc-imageselect"]/div[3]/div[2]/div[1]/div[1]/div[4]')
            buster_button.click()
            time.sleep(4)
        except NoSuchElementException:
            self.driver.quit()
            exit()

        self.driver.switch_to.default_content()
        time.sleep(8)

    def quit(self):
        if self.driver:
            self.driver.quit()

class BNMPScraper:
    def __init__(self, cookies, driver):
        self.cookies = cookies
        self.params = {'page': '0', 'size': str(MAX_ITEMS_PER_PAGE), 'sort': ''}
        self.json_data = {'buscaOrgaoRecursivo': False, 'orgaoExpeditor': {}, 'idEstado': 25}
        self.driver = driver

    def make_request(self):
        try:
            response = requests.post(
                BNMP_URL,
                params=self.params,
                cookies=self.cookies,
                headers=HEADERS,
                json=self.json_data,
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            return None

    def scrape(self):
        page = 0
        page_count = 0
        all_data = []

        while True:
            self.params['page'] = str(page)
            response = self.make_request()
            if not response or response.status_code != 200:
                break

            items = response.json().get('content', [])
            all_data.extend(items)
            page += 1
            page_count += 1

            if page_count == RENEW_REQUEST_THRESHOLD:
                page_count = 0
                print("Renewing request...")
                self.random_sleep()  # Random sleep before refreshing the page
                self.driver.refresh()  # Refreshing the page

            print(f"Successfully processed page {page}")

            if len(items) < MAX_ITEMS_PER_PAGE:
                break

        self.save_json(all_data)
        self.save_excel(all_data)

    def random_sleep(self):
        time.sleep(random.uniform(0, 3))  # Sleep for a random time between 0 and 3 seconds

    @staticmethod
    def save_json(data):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    @staticmethod
    def save_excel(data):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df = pd.DataFrame(data).drop(columns=['dataExpedicao', 'dataNascimento'], errors='ignore')
        df.to_excel(EXCEL_FILE, index=False)
        print(f"Arquivo '{EXCEL_FILE}' criado com sucesso.")

# Example of usage
if __name__ == "__main__":
    chrome_driver_path = "/usr/bin/chromedriver"  # Make sure the path is correct
    chrome_extension_path = "buster.crx"  # Path to Buster extension

    recaptcha_bypasser = ReCaptchaBypasser(chrome_driver_path, chrome_extension_path)
    recaptcha_bypasser.initialize_driver()
    recaptcha_bypasser.bypass_recaptcha("https://portalbnmp.cnj.jus.br/#/captcha/")

    # Fetching cookies after solving CAPTCHA
    cookies = recaptcha_bypasser.driver.get_cookies()
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

    scraper = BNMPScraper(cookies_dict, recaptcha_bypasser.driver)
    scraper.scrape()

    # Closing the browser window
    recaptcha_bypasser.quit()
