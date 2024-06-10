import os
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup

# Cria o diretório 'output' se ele não existir
if not os.path.exists('output'):
    os.makedirs('output')


class BNMPScraper:  # Define a classe BNMPScraper
    def __init__(self):  # Método de inicialização da classe
        # Define o valor do cookie necessário para acessar a página
        self.cookie_value = ''
        self.cookies = {
            'portalbnmp': self.cookie_value,  # Define o cookie no formato necessário para fazer a requisição
        }

        self.headers = {  # Define os cabeçalhos HTTP para simular um navegador
            'User-Agent': (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, como Gecko) Chrome/86.0.4240.198 Safari/537.36")
        }

        self.params = {  # Define os parâmetros da requisição, como página, tamanho e ordenação
            'page': '0',
            'size': '30',
            'sort': '',
        }

        self.json_data = {  # Define os dados em formato JSON para a requisição
            'buscaOrgaoRecursivo': False,
            'orgaoExpeditor': {},
            'idEstado': 25,
        }

    def fazer_requisicao(self):  # Método para fazer a requisição HTTP
        try:
            # Envia uma requisição POST para a URL especificada com os parâmetros, cookies, cabeçalhos e dados JSON
            response = requests.post(
                'https://portalbnmp.cnj.jus.br/bnmpportal/api/pesquisa-pecas/filter',
                params=self.params,
                cookies=self.cookies,
                headers=self.headers,
                json=self.json_data,
            )
            response.raise_for_status()  # Lança uma exceção se a requisição falhar
            return response  # Retorna a resposta da requisição
        except requests.exceptions.RequestException as e:  # Trata exceções de requisição
            print(f"Erro ao fazer a requisição: {e}")  # Imprime uma mensagem de erro
            return None  # Retorna None em caso de erro

    def executar(self):  # Método para executar o scraper
        page = 0  # Inicializa a variável de página como 0
        page_count = 0  # Inicializa o contador de página como 0
        with open('output/1.dados_gerais.txt', 'w') as f:  # Abre o arquivo 'dados_gerais.txt' para escrita
            while True:  # Loop infinito
                self.params['page'] = str(page)  # Define o número da página nos parâmetros da requisição
                response = self.fazer_requisicao()  # Faz a requisição HTTP
                if response is None or response.status_code != 200:  # Verifica se a resposta é válida
                    break  # Sai do loop se a resposta for inválida

                # Analisa a resposta JSON para obter os itens da página
                response_data = response.json()
                items = response_data.get('content', [])

                # Cria um objeto BeautifulSoup para analisar o HTML da resposta
                soup = BeautifulSoup(response.text, 'html.parser')

                # Salva o HTML da página no arquivo de saída
                f.write(f"\n\n--- Página {page} ---\n\n")
                f.write(soup.prettify())

                page += 1  # Incrementa o número da página
                page_count += 1  # Incrementa o contador de página

                # Renova a requisição a cada 40 páginas para evitar timeout ou bloqueio
                if page_count == 40:
                    page_count = 0  # Reinicia o contador de página
                    print("Renovando a requisição...")  # Imprime uma mensagem informativa
                    self.cookies['portalbnmp'] = self.cookie_value  # Renova o cookie
                    continue  # Reinicia o loop

                print(f"Página {page} processada com sucesso")  # Imprime uma mensagem de sucesso

                # Verifica se a página atual tem menos de 30 itens
                if len(items) < 30:
                    break  # Sai do loop se a página não contiver mais de 30 itens

# Instancia o scraper BNMPScraper
scraper = BNMPScraper()
# Executa o scraper
scraper.executar()

# Função para ler e processar o arquivo dados_gerais.txt
def ler_arquivo(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        json_blocks = []
        current_block = []
        
        for line in lines:
            if line.startswith('--- Página'):
                if current_block:
                    json_blocks.append(''.join(current_block).strip())
                    current_block = []
            else:
                current_block.append(line.strip())

        if current_block:
            json_blocks.append(''.join(current_block).strip())
        
        all_records = []
        for block in json_blocks:
            try:
                data = json.loads(block)
                all_records.extend(data.get('content', []))
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON: {e}")
        
        return all_records


# Caminho para o arquivo dados_gerais.txt
arquivo = 'output/1.dados_gerais.txt'

# Ler o conteúdo do arquivo
conteudo = ler_arquivo(arquivo)

if conteudo:
    # Crie um DataFrame a partir da lista de dicionários, excluindo colunas indesejadas
    df = pd.DataFrame(conteudo).drop(columns=['dataExpedicao', 'dataNascimento'], errors='ignore')
    
    # Salve o DataFrame em um arquivo Excel
    df.to_excel('output/2.dados_gerais.xlsx', index=False)
    print("Arquivo 'dados_gerais.xlsx' criado com sucesso.")
else:
    print("Nenhum dado encontrado no conteúdo JSON.")

# Carrega o arquivo xlsx
dados_gerais = pd.read_excel('output/2.dados_gerais.xlsx')

# Extrai os IDs da primeira coluna (índice 0)
ids = dados_gerais.iloc[1:, 0].tolist()

# Define os cookies e headers
cookies = {
    'portalbnmp': scraper.cookie_value,
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
}

# Função para obter resposta para cada ID
def obter_resposta_id(id, cookies, headers):
    # Faz a solicitação HTTP para obter os dados
    response = requests.get(f'https://portalbnmp.cnj.jus.br/bnmpportal/api/certidaos/{id}/10', cookies=cookies, headers=headers)
    
    # Verifica se a resposta é bem-sucedida
    if response.status_code == 200:
        # Retorna a resposta
        return response.text
    else:
        return f"Erro ao obter dados para o ID {id}: {response.status_code}"

# Lista para armazenar todas as respostas
todas_respostas = []

# Para cada ID, obter a resposta e adicioná-la à lista
for i, id in enumerate(ids):
    resposta_id = obter_resposta_id(id, cookies, headers)
    todas_respostas.append(resposta_id)
    
    # Renova a requisição a cada 10 IDs para evitar timeout ou bloqueio
    if i % 10 == 0:
        print("Renovando a requisição...")  # Imprime uma mensagem informativa
        cookies['portalbnmp'] = scraper.cookie_value  # Renova o cookie

# Salva todas as respostas em um único arquivo de texto
with open('output/3.todas_respostas.txt', 'w') as arquivo:
    for resposta in todas_respostas:
        arquivo.write(resposta + '\n')

print("Todas as respostas salvas com sucesso em 'todas_respostas.txt'")