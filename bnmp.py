import os
import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import random

# Diretório e arquivos de saída / Output directory and files
OUTPUT_DIR = 'output'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, '1.general_data.json')
EXCEL_FILE = os.path.join(OUTPUT_DIR, '2.general_data.xlsx')
RESPONSES_FILE = os.path.join(OUTPUT_DIR, '3.all_responses.json')

# URL da API BNMP e constantes / BNMP API URL and constants
BNMP_URL = 'https://portalbnmp.cnj.jus.br/bnmpportal/api/pesquisa-pecas/filter'
MAX_ITEMS_PER_PAGE = 30
RENEW_REQUEST_THRESHOLD = 40
REFRESH_THRESHOLD = 200
MAX_REQUESTS_BEFORE_PAUSE = 50

# Cabeçalhos HTTP / HTTP headers
HEADERS = {
    'User-Agent': (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, como Gecko) Chrome/86.0.4240.198 Safari/537.36"
    )
}

# Mapeamento de descrições de peças para seus IDs / Mapping of piece descriptions to their IDs
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
        self.json_data = {'recursiveSearchAgency': False, 'issuingAgency': {}, 'stateId': 25}
        self.driver = driver
        self.processed_ids_count = 0  # Inicializa o contador de IDs processados / Initialize processed IDs counter
        self.request_count = 0  # Inicializa o contador de requisições / Initialize request counter

    def make_request(self):
        """
        Faz uma requisição POST para a API da BNMP e trata exceções.
        Make a POST request to BNMP API and handle exceptions.
        """
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
            print(f"Erro ao fazer a requisição: {e}")
            print(f"Error making request: {e}")
            return None

    @staticmethod
    def random_pause():
        """
        Pausa por uma duração aleatória entre as requisições.
        Pause for a random duration between requests.
        """
        time.sleep(random.uniform(1, 3))  # Pausa entre 1 e 3 segundos / Pause between 1 and 3 seconds

    def scrape(self):
        """
        Extrai dados da BNMP, salva arquivos JSON e Excel, e gerencia paginação e atualização.
        Scrape data from BNMP, save JSON and Excel files, and handle pagination and refreshing.
        """
        page = 0
        page_count = 0
        all_data = []
        consecutive_401_count = 0  # Contador de respostas 401 consecutivas / Counter for consecutive 401 responses

        while True:
            self.params['page'] = str(page)
            response = self.make_request()
            self.request_count += 1

            if self.request_count % 60 == 0:
                print(f"Pausando após {self.request_count} requisições...")
                print(f"Pausing after {self.request_count} requests...")
                time.sleep(random.uniform(60, 120))  # Pausa entre 1 e 2 minutos / Pause between 1 and 2 minutes

            if not response:
                break

            if response.status_code == 401:
                consecutive_401_count += 1
                if consecutive_401_count >= 5:  # Define um limite para a detecção de bloqueio / Set a limit for blocking detection
                    print("Bloqueado pelo servidor. Encerrando o processo de extração.")
                    print("Blocked by server. Exiting scraping process.")
                    break
            else:
                consecutive_401_count = 0  # Reseta o contador / Reset the counter

            if response.status_code != 200:
                print(f"Código de status inesperado: {response.status_code}. Encerrando o processo de extração.")
                print(f"Unexpected status code: {response.status_code}. Exiting scraping process.")
                break

            items = response.json().get('content', [])
            all_data.extend(items)
            page += 1
            page_count += 1

            if page_count == RENEW_REQUEST_THRESHOLD:
                page_count = 0
                print("Renovando requisição...")
                print("Renewing request...")
                self.driver.refresh()  # Atualiza a página / Refreshing the page

            print(f"Página {page} processada com sucesso.")
            print(f"Successfully processed page {page}")

            if len(items) < MAX_ITEMS_PER_PAGE:
                break

            self.random_pause()

        self.save_json(all_data)
        self.save_excel(all_data)
        self.refresh_browser()
        self.process_excel()

    def process_excel(self):
        """
        Processa o arquivo Excel para mapear descrições de peças para seus IDs e buscar dados.
        Process the Excel file to map piece descriptions to their IDs and fetch data.
        """
        df = pd.read_excel(EXCEL_FILE)
        df['peca_id'] = df['descricaoPeca'].map(PECA_MAP)

        for idx, row in df.iterrows():
            id_value = row['id']
            piece_description = row['descricaoPeca']
            piece_id = row['peca_id']

            if piece_id:
                response = self.fetch_data_by_id_and_piece(id_value, piece_id)
                result = {
                    "id": id_value,
                    "piece": piece_description,
                    "response": response
                }
            else:
                result = {
                    "id": id_value,
                    "piece": piece_description,
                    "error": f"Peça '{piece_description}' não encontrada no dicionário de peças."
                              f"Piece '{piece_description}' not found in piece dictionary."
                }
            self.save_response(result)

            self.processed_ids_count += 1
            if self.processed_ids_count % REFRESH_THRESHOLD == 0:
                self.refresh_browser()

    def fetch_data_by_id_and_piece(self, id_value, piece_id):
        """
        Busca dados pelo ID e ID da peça na API da BNMP e trata erros.
        Fetch data by ID and piece ID from BNMP API and handle errors.
        """
        html_url = f'https://portalbnmp.cnj.jus.br/#/resumo-peca/{id_value}/{piece_id}/%2Fpesquisa-peca'
        json_url = f'https://portalbnmp.cnj.jus.br/bnmpportal/api/certidaos/{id_value}/{piece_id}'

        html_response = requests.get(html_url, cookies=self.cookies, headers=HEADERS)
        if html_response.status_code == 200:
            json_response = requests.get(json_url, cookies=self.cookies, headers=HEADERS)
            if json_response.status_code == 200:
                try:
                    return json_response.json()
                except json.JSONDecodeError:
                    return {"erro": f"Erro ao decodificar JSON para ID {id_value}, ID da peça {piece_id}. "
                                    f"Resposta: {json_response.text}",
                            "error": f"Error decoding JSON for ID {id_value}, Piece ID {piece_id}. "
                                     f"Response: {json_response.text}"}
            else:
                return {"erro": f"Erro ao obter dados JSON para ID {id_value}, ID da peça {piece_id}: "
                                f"Código de status {json_response.status_code}",
                        "error": f"Error getting JSON data for ID {id_value}, Piece ID {piece_id}: "
                                 f"Status {json_response.status_code}"}
        else:
            return {"erro": f"Erro ao obter HTML para ID {id_value}, ID da peça {piece_id}: "
                            f"Código de status {html_response.status_code}",
                    "error": f"Error getting HTML for ID {id_value}, Piece ID {piece_id}: "
                             f"Status {html_response.status_code}"}

    def refresh_browser(self):
        """
        Atualiza a página do portal BNMP usando Selenium WebDriver.
        Refresh the BNMP portal page using Selenium WebDriver.
        """
        self.driver.get('https://portalbnmp.cnj.jus.br/')
        time.sleep(2)  # Espera 2 segundos para a página carregar completamente / Wait 2 seconds for the page to fully load

    def save_response(self, result):
        """
        Salva a resposta individual em um arquivo JSON.
        Save individual response to a JSON file.
        """
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(RESPONSES_FILE, 'a', encoding='utf-8') as file:
            json.dump(result, file, ensure_ascii=False)
            file.write('\n')

    def save_json(self, data):
        """
        Salva os dados extraídos em um arquivo JSON.
        Save scraped data to a JSON file.
        """
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    @staticmethod
    def save_excel(data):
        """
        Salva os dados extraídos em um arquivo Excel.
        Save scraped data to an Excel file.
        """
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df = pd.DataFrame(data).drop(columns=['dataExpedicao', 'dataNascimento'], errors='ignore')
        df.to_excel(EXCEL_FILE, index=False)
        print(f"Arquivo '{EXCEL_FILE}' criado com sucesso.")
        print(f"File '{EXCEL_FILE}' created successfully.")

if __name__ == "__main__":
    chrome_driver_path = "/usr/bin/chromedriver"

    chrome_options = Options()
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.get("https://portalbnmp.cnj.jus.br/#/captcha/")

    input("Pressione Enter após resolver o CAPTCHA manualmente...")
    input("Press Enter after you have solved the CAPTCHA manually...")

    cookies = driver.get_cookies()
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

    scraper = BNMPScraper(cookies_dict, driver)
    scraper.scrape()

    driver.quit()
