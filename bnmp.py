import os
import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
import time
import random

OUTPUT_DIR = 'output'
OUTPUT_FILE = 'output/1.dados_gerais.json'
EXCEL_FILE = 'output/2.dados_gerais.xlsx'
BNMP_URL = 'https://portalbnmp.cnj.jus.br/bnmpportal/api/pesquisa-pecas/filter'
MAX_ITEMS_PER_PAGE = 30
RENEW_REQUEST_THRESHOLD = 40
REFRESH_THRESHOLD = 300
RESPONSES_FILE = 'output/3.todas_respostas.json'  # Novo arquivo de saída

HEADERS = {
    'User-Agent': (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    )
}
PECA_MAP = {
    "Mandado de Prisão": 1,
    "Contramandado": 2,
    "Guia de Recolhimento": 3,
    "Guia de Internamento": 4,
    "Alvará de Soltura": 5,
    "Documento de Desinternamento": 6,
    "Certidão de Cumprimento das Prisões": 7,
    "Certidão de Extinção de Punibilidade": 8,
    "Certidão de Cumprimentos das Internações": 9,
    "Mandado de Internação": 10,
    "Guia de Recolhimento (Acervo da Execução)": 11,
    "Certidão de arquivamento de guia": 12,
    "Guia de Internação (Acervo da Execução)": 13,
    "Certidão de Alteração de Unidade ou Regime Prisional": 14
}

class BNMPScraper:
    def __init__(self, cookies, driver):
        self.cookies = cookies
        self.params = {'page': '0', 'size': str(MAX_ITEMS_PER_PAGE), 'sort': ''}
        self.json_data = {'buscaOrgaoRecursivo': False, 'orgaoExpeditor': {}, 'idEstado': 25}
        self.driver = driver
        self.processed_ids_count = 0  # Inicializa o contador de IDs processados

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
        df = pd.DataFrame(all_data)

        # Loop para processar IDs e peças e salvar as respostas
        for idx, row in df.iterrows():  # Itera sobre as linhas do DataFrame
            id = row['id']
            peca_descricao = row['descricaoPeca']
            peca_id = PECA_MAP.get(peca_descricao)

            if peca_id:
                response = self.fetch_data_by_id_and_peca(id, peca_id)
                result = {
                    "id": id,
                    "peca": peca_descricao,
                    "response": response
                }
            else:
                result = {
                    "id": id,
                    "peca": peca_descricao,
                    "error": f"Peça '{peca_descricao}' não encontrada no dicionário de peças."
                }
            save_response(result, RESPONSES_FILE)

    def fetch_data_by_id_and_peca(self, id, peca_id):
        all_responses = []
        page = 0
        while True:
            url = f'https://portalbnmp.cnj.jus.br/bnmpportal/api/certidaos/{id}/{peca_id}?page={page}'
            response = requests.get(url, cookies=self.cookies, headers=HEADERS)
            status_code = response.status_code
            print(f"Requisição para o ID {id}, Peça ID {peca_id}, Página {page} feita... Status {status_code}")

            if status_code == 200:
                response_data = response.json()
                all_responses.extend(response_data)

                if len(response_data) < MAX_ITEMS_PER_PAGE:
                    break  # No more pages to fetch

                page += 1  # Move to the next page
            else:
                return {"error": f"Erro ao obter dados para o ID {id}, Peça ID {peca_id}, Página {page}: Status {status_code}"}

        self.processed_ids_count += 1
        if self.processed_ids_count % REFRESH_THRESHOLD == 0:
            self.move_mouse_and_click()
            self.driver.refresh()
            self.random_sleep()
        
        return all_responses

    def random_sleep(self):
        time.sleep(random.uniform(0, 3))  # Sleep for a random time between 0 and 3 seconds

    def move_mouse_and_click(self):
        action = ActionChains(self.driver)
        # Movimenta o mouse
        for _ in range(5):
            action.move_by_offset(random.randint(-10, 10), random.randint(-10, 10)).perform()
            time.sleep(1)
        # Clique no meio da tela
        width = self.driver.execute_script("return window.innerWidth")
        height = self.driver.execute_script("return window.innerHeight")
        action.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, 'body'), width / 2, height / 2).click().perform()

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

def save_response(result, file_path):
    with open(file_path, 'a', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False)
        file.write('\n')  # Adiciona uma nova linha após cada resultado

if __name__ == "__main__":
    chrome_driver_path = "/usr/bin/chromedriver"  # Make sure the path is correct

    # Initialize the driver
    chrome_options = Options()
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Open the CAPTCHA page
    driver.get("https://portalbnmp.cnj.jus.br/#/captcha/")

    # Manual step: Wait for the user to solve the CAPTCHA
    input("Press Enter after you have solved the CAPTCHA manually...")

    # Fetching cookies after solving CAPTCHA
    cookies = driver.get_cookies()
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

    # Initialize scraper with cookies and driver
    scraper = BNMPScraper(cookies_dict, driver)
    scraper.scrape()

    # Closing the browser window
    driver.quit()
