import os
import json
import pandas as pd
import requests
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.by import By  # Importação do By adicionada
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
import time
import random
import pyautogui

OUTPUT_DIR = 'output'
OUTPUT_FILE = 'output/1.dados_gerais.json'
EXCEL_FILE = 'output/2.dados_gerais.xlsx'
BNMP_URL = 'https://portalbnmp.cnj.jus.br/bnmpportal/api/pesquisa-pecas/filter'
MAX_ITEMS_PER_PAGE = 30
RENEW_REQUEST_THRESHOLD = 40
REFRESH_THRESHOLD = 40
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
        consecutive_401_count = 0  # Contador de respostas 401 consecutivas

        while True:
            self.params['page'] = str(page)
            response = self.make_request()
            if not response:
                break

            if response.status_code == 401:
                consecutive_401_count += 1
                if consecutive_401_count >= 5:  # Defina um limite para a detecção de bloqueio
                    print("Blocked by server. Exiting scraping process.")
                    break
            else:
                consecutive_401_count = 0  # Reiniciar o contador

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
                self.driver.refresh()  # Refreshing the page

            print(f"Successfully processed page {page}")

            if len(items) < MAX_ITEMS_PER_PAGE:
                break

        self.save_json(all_data)
        self.save_excel(all_data)
        self.refresh_browser()
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
            if self.processed_ids_count % REFRESH_THRESHOLD == 0:
                self.refresh_browser()

    def fetch_data_by_id_and_peca(self, id_valor, peca_id):
        html_url = f'https://portalbnmp.cnj.jus.br/#/resumo-peca/{id_valor}/{peca_id}/%2Fpesquisa-peca'
        json_url = f'https://portalbnmp.cnj.jus.br/bnmpportal/api/certidaos/{id_valor}/{peca_id}'

        # Faz a requisição para a página HTML
        html_response = requests.get(html_url, cookies=self.cookies, headers=HEADERS)
        html_status_code = html_response.status_code
        print(f"Requisição HTML para o ID {id_valor}, Peça ID {peca_id} feita... Status {html_status_code}")

        if html_status_code == 200:
            # Faz a requisição para a URL JSON
            json_response = requests.get(json_url, cookies=self.cookies, headers=HEADERS)
            json_status_code = json_response.status_code
            print(f"Requisição JSON para o ID {id_valor}, Peça ID {peca_id} feita... Status {json_status_code}")

            if json_status_code == 200:
                try:
                    return json_response.json()
                except json.JSONDecodeError:
                    return {"error": f"Erro ao decodificar JSON para o ID {id_valor}, Peça ID {peca_id}. Resposta: {json_response.text}"}
            else:
                return {"error": f"Erro ao obter dados JSON para o ID {id_valor}, Peça ID {peca_id}: Status {json_status_code}"}
        else:
            return {"error": f"Erro ao obter HTML para o ID {id_valor}, Peça ID {peca_id}: Status {html_status_code}"}

    def refresh_browser(self):
        url = 'https://portalbnmp.cnj.jus.br/'
        self.driver.get(url)
        time.sleep(2)  # Espera 2 segundos para carregar a página completamente
        
        # Digita a URL e pressiona Enter usando Selenium
        input_field = self.driver.find_element(By.XPATH, '//input[@type="text"]')
        input_field.send_keys(url + Keys.RETURN)
        
        # Movimento de deslizamento na página por 5 segundos com PyAutoGUI
        for _ in range(10):  # 10 passos para deslizar a página
            pyautogui.scroll(-100)  # Desliza 100 pixels para cima
            time.sleep(0.5)  # Intervalo de 0.5 segundos entre cada passo

    def random_sleep(self):
        time.sleep(random.uniform(1, 30))

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
    chrome_driver_path = "/usr/bin/chromedriver"

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

#LIMPEZA DOS DADOS

# Função para normalizar a coluna 'enderecos'
def normalizar_enderecos(enderecos):
    if isinstance(enderecos, list) and len(enderecos) > 0:
        endereco = enderecos[0]  # Considerando o primeiro endereço da lista
        logradouro = endereco.get('logradouro', '')
        bairro = endereco.get('bairro', '')
        numero = endereco.get('numero', '')
        municipio = endereco.get('municipio', {}).get('nome', '')
        estado = endereco.get('estado', {}).get('sigla', '')
        return f"{logradouro}, {numero}, {bairro}, {municipio}/{estado}"
    return ''

# Função para processar cada objeto JSON individualmente
def process_json_line(json_line):
    item = json.loads(json_line)
    id = item.get('id', '')
    response = item.get('response', {})
    pessoa = response.get('pessoa', {})
    enderecos = pessoa.get('enderecos', [])
    endereco_1 = normalizar_enderecos(enderecos)
    tipificacao_penal = [tp.get('rotulo', '') for tp in response.get('tipificacaoPenal', [])]

    return {
        "id": id,
        "tipificacaoPenal": tipificacao_penal,
        "endereco_1": endereco_1 if endereco_1 else ''
    }

# Lendo o arquivo JSON linha por linha
file_path = 'output/3.todas_respostas.json'
dados = []

with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:  # Ignorar linhas vazias
            dados.append(process_json_line(line))

# Criando o DataFrame
df = pd.DataFrame(dados)

# Alterações nas colunas 'endereco_1' e 'tipificacaoPenal'
df['endereco_1'] = df['endereco_1'].str.replace('/None', '')
df['endereco_1'] = df['endereco_1'].str.replace('None,', '')
df['endereco_1'] = df['endereco_1'].str.replace(', None', '')
df['endereco_1'] = df['endereco_1'].str.replace(', ,', ',')
df['endereco_1'] = df['endereco_1'].str.upper()
def extract_first_tipificacao(tipificacoes):
    if tipificacoes:
        first_tipificacao = tipificacoes[0]
        return first_tipificacao.split(';')[0]
    return ''

df['tipificacaoPenal'] = df['tipificacaoPenal'].apply(lambda x: [extract_first_tipificacao(x)])

# Salvando em um arquivo Excel
output_file_path = 'output/4.dados_finais.xlsx'
df.to_excel(output_file_path, index=False)


# Carregue os dados dos arquivos
dados_gerais = pd.read_excel('output/2.dados_gerais.xlsx')
dados_finais = pd.read_excel('output/4.dados_finais.xlsx')

# Mesclar os DataFrames com base no ID
dados_gerais_finais = pd.merge(dados_gerais, dados_finais, on='ID', how='inner')

# Salve o resultado em um novo arquivo
dados_gerais_finais.to_excel('4.dados_gerais_finais.xlsx', index=False)
print("Dados mesclados e salvos em 4.dados_gerais_finais.xlsx.")

# Lista de arquivos a serem excluídos
arquivos_para_excluir = [
    'output/1.dados_gerais.json',
    'output/2.dados_gerais.xlsx',
    'output/3.todas_respostas.json',
    'output/4.dados_finais.xlsx'
]

# Exclua cada arquivo
for arquivo in arquivos_para_excluir:
    try:
        os.remove(arquivo)
        print(f"Arquivo {arquivo} excluído com sucesso.")
    except FileNotFoundError:
        print(f"Arquivo {arquivo} não encontrado.")

print("Todos os arquivos foram excluídos conforme solicitado.")


print('Processo Finalizado')


