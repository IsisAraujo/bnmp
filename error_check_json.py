import json
import os
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# Diretório de saída e arquivo Excel
OUTPUT_DIR = 'output'
EXCEL_FILE = 'output/4.dados_erros.xlsx'
RESPONSES_FILE = 'output/4.1dados_erros.json'

# URL da API da BNMP e constantes
BNMP_URL = 'https://portalbnmp.cnj.jus.br/bnmpportal/api/pesquisa-pecas/filter'
MAX_ITEMS_PER_PAGE = 30
REFRESH_THRESHOLD = 200
MAX_REQUESTS_BEFORE_PAUSE = 60

# Cabeçalhos HTTP
HEADERS = {
    'User-Agent': (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    )
}

# Mapeamento de descrições de peças para seus IDs
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
        self.driver = driver
        self.processed_ids_count = 0
        self.request_count = 0  # Contador de requisições

    def fetch_data_by_id_and_peca(self, id_valor, peca_id):
        """
        Fetch data by ID and piece ID from BNMP API and handle errors.
        """
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
                    elif json_status_code == 401:  # Captcha required
                        self.handle_captcha()
                        continue
                    else:
                        return {"error": f"Erro ao obter dados JSON para o ID {id_valor}, Peça ID {peca_id}: Status {json_status_code}"}
                elif html_status_code == 401:  # Captcha required
                    self.handle_captcha()
                    continue
                else:
                    return {"error": f"Erro ao obter HTML para o ID {id_valor}, Peça ID {peca_id}: Status {html_status_code}"}
            except requests.RequestException as e:
                print(f"Tentativa {attempt + 1} falhou: {e}. Retentando em {backoff_factor ** attempt} segundos.")

        return {"error": f"Falha ao obter dados para o ID {id_valor}, Peça ID {peca_id} após {max_retries} tentativas."}

    def handle_captcha(self):
        """
        Handle CAPTCHA by prompting the user to solve it manually.
        """
        self.driver.get("https://portalbnmp.cnj.jus.br/#/captcha/")
        input("Por favor, resolva o CAPTCHA e pressione Enter para continuar...")
        self.cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}

    def save_response(self, result):
        """
        Save individual response to a JSON file.
        """
        with open(RESPONSES_FILE, 'a', encoding='utf-8') as file:
            json.dump(result, file, ensure_ascii=False)
            file.write('\n')

    def scrape(self):
        """
        Scrape data from BNMP, save JSON and handle pagination and refreshing.
        """
        df = pd.read_excel(EXCEL_FILE)
        df['peca_id'] = df['peca'].map(PECA_MAP)

        for idx, row in df.iterrows():
            id_valor = row['id']
            descricao_peca = row['peca']
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
            self.request_count += 1  # Incrementa o contador de requisições

            if self.request_count % MAX_REQUESTS_BEFORE_PAUSE == 0:
                print(f"Pausando após {self.request_count} requisições...")

            if self.processed_ids_count % REFRESH_THRESHOLD == 0:
                print(f"Processados {self.processed_ids_count} IDs, atingido limite de atualizações.")

if __name__ == "__main__":
    chrome_driver_path = "C:\webdriver\chromedriver.exe"

    chrome_options = Options()
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.get("https://portalbnmp.cnj.jus.br/#/captcha/")
    input("Pressione Enter após resolver o CAPTCHA manualmente...")

    cookies = driver.get_cookies()
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

    scraper = BNMPScraper(cookies_dict, driver)
    scraper.scrape()

    # Fechar o navegador após a conclusão
    driver.quit()
