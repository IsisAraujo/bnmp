import os
import json
import pandas as pd
import requests
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from openpyxl.workbook import Workbook
import time
import random
import subprocess 

OUTPUT_DIR = 'output'
OUTPUT_FILE = 'output/1.dados_gerais.json'
EXCEL_FILE = 'output/2.dados_gerais.xlsx'
BNMP_URL = 'https://portalbnmp.cnj.jus.br/bnmpportal/api/pesquisa-pecas/filter'
MAX_ITEMS_PER_PAGE = 30
RENEW_REQUEST_THRESHOLD = 40
MAX_ITEMS_PER_PAGE = 30
REFRESH_THRESHOLD = 40
MAX_REQUESTS_BEFORE_PAUSE = 50
RESPONSES_FILE = 'output/3.todas_respostas.json'

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
        self.processed_ids_count = 0
        self.request_count = 0

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
        consecutive_401_count = 0

        while True:
            self.params['page'] = str(page)
            response = self.make_request()
            if not response:
                break

            if response.status_code == 401:
                consecutive_401_count += 1
                if consecutive_401_count >= 5:
                    print("Blocked by server. Exiting scraping process.")
                    break
            else:
                consecutive_401_count = 0

            if response.status_code != 200:
                print(f"Unexpected status code: {response.status_code}. Exiting scraping process.")
                break

            items = response.json().get('content', [])
            all_data.extend(items)
            page += 1
            page_count += 1

            if page_count == RENEW_REQUEST_THRESHOLD:
                page_count = 0
                print("Renewing request...")
                self.driver.refresh()

            print(f"Successfully processed page {page}")

            if len(items) < MAX_ITEMS_PER_PAGE:
                break
            if self.processed_ids_count >= 30:
                print("Limite de 30 entradas atingido. Encerrando o processo de scraping.")
                break
        self.save_json(all_data)
        self.save_excel(all_data)
        df = pd.read_excel(EXCEL_FILE)
        df['peca_id'] = df['descricaoPeca'].map(PECA_MAP)

        for idx, row in df.iterrows():


            id_valor = row['id']
            descricao_peca = row['descricaoPeca']
            peca_id = row['peca_id']

            if peca_id:
                response = self.fetch_data_by_id_and_peca(id_valor, peca_id)
                result = {
                    "id": id_valor,
                    "peca": descricao_peca,
                    "response": response
                }
            else:
                result = {
                    "id": id_valor,
                    "peca": descricao_peca,
                    "error": f"Peça '{descricao_peca}' não encontrada no dicionário de peças."
                }
            self.save_response(result)

            self.processed_ids_count += 1
            self.request_count += 1

            
            if self.request_count % MAX_REQUESTS_BEFORE_PAUSE == 0:
                print(f"Pausando após {self.request_count} requisições...")

            if self.processed_ids_count % REFRESH_THRESHOLD == 0:
                self.refresh_browser()

    def fetch_data_by_id_and_peca(self, id_valor, peca_id):
        html_url = f'https://portalbnmp.cnj.jus.br/#/resumo-peca/{id_valor}/{peca_id}/%2Fpesquisa-peca'
        json_url = f'https://portalbnmp.cnj.jus.br/bnmpportal/api/certidaos/{id_valor}/{peca_id}'

        max_retries = 5
        backoff_factor = 2

        for attempt in range(max_retries):
            try:
                html_response = requests.get(html_url, cookies=self.cookies, headers=HEADERS)
                html_status_code = html_response.status_code
                print(f"Requisição HTML para o ID {id_valor}, Peça ID {peca_id} feita... Status {html_status_code}")

                if html_status_code == 200:
                    json_response = requests.get(json_url, cookies=self.cookies, headers=HEADERS)
                    json_status_code = json_response.status_code
                    print(f"Requisição JSON para o ID {id_valor}, Peça ID {peca_id} feita... Status {json_status_code}")

                    if json_status_code == 200:
                        try:
                            return json_response.json()
                        except json.JSONDecodeError:
                            return {"error": f"Erro ao decodificar JSON para o ID {id_valor}, Peça ID {peca_id}. Resposta: {json_response.text}"}
                    elif json_status_code == 401:
                        self.handle_captcha()
                        continue
                    else:
                        return {"error": f"Erro ao obter dados JSON para o ID {id_valor}, Peça ID {peca_id}: Status {json_status_code}"}
                elif html_status_code == 401:
                    self.handle_captcha()
                    continue
                else:
                    return {"error": f"Erro ao obter HTML para o ID {id_valor}, Peça ID {peca_id}: Status {html_status_code}"}
            except requests.RequestException as e:
                print(f"Tentativa {attempt + 1} falhou: {e}. Retentando em {backoff_factor ** attempt} segundos.")
                time.sleep(backoff_factor ** attempt)

        return {"error": f"Falha ao obter dados para o ID {id_valor}, Peça ID {peca_id} após {max_retries} tentativas."}

    def refresh_browser(self):
        url = 'https://portalbnmp.cnj.jus.br/'
        self.driver.get(url)
        time.sleep(10)  # Esperar 10 segundos para garantir que a página carregue

    def handle_captcha(self):
        self.driver.get("https://portalbnmp.cnj.jus.br/#/captcha/")
        input("Por favor, resolva o CAPTCHA e pressione Enter para continuar...")
        self.cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}

    def save_response(self, result):
        with open(RESPONSES_FILE, 'a', encoding='utf-8') as file:
            json.dump(result, file, ensure_ascii=False)
            file.write('\n')

    def save_json(self, data):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
  
    @staticmethod
    def save_excel(data):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df = pd.DataFrame(data).drop(columns=['dataExpedicao', 'dataNascimento'], errors='ignore')
        df.to_excel(EXCEL_FILE, index=False)
        print(f"Arquivo '{EXCEL_FILE}' criado com sucesso.")

if __name__ == "__main__":
    #chrome_driver_path = "/usr/bin/chromedriver"para linux
    chrome_driver_path = "C:\webdriver\chromedriver.exe"

    chrome_options = Options()
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.get("https://portalbnmp.cnj.jus.br/#/captcha/")

    input("Press Enter after you have solved the CAPTCHA manually...")

    cookies = driver.get_cookies()
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

    scraper = BNMPScraper(cookies_dict, driver)
    scraper.scrape()

    driver.quit()

    # Executar o script de limpeza após a raspagem
    print("Executando analise de erros...")
    subprocess.run(["python", "error_check.py"])

    print('Processo Finalizado')

