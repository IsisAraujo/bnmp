import pandas as pd
import json
from openpyxl.workbook import Workbook

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

# Função para buscar o CPF no documento
def buscar_cpf(documentos):
    for documento in documentos:
        if documento.get('tipoDocumento', {}).get('descricao') == 'CPF':
            return documento.get('numero')
    return None

# Função para processar cada objeto JSON individualmente
def process_json_line(json_line):
    item = json.loads(json_line)
    id = item.get('id', '')
    response = item.get('response', {})
    
    # Verificar se há um erro na resposta
    if 'error' in response:
        return None, {
            "id": id,
            "peca": item.get('peca', ''),
            "error": response.get('error', '')
        }
    
    pessoa = response.get('pessoa', {})
    enderecos = pessoa.get('enderecos', [])
    endereco_1 = normalizar_enderecos(enderecos)
    tipificacao_penal = [tp.get('rotulo', '') for tp in response.get('tipificacaoPenal', [])]
    cpf = buscar_cpf(pessoa.get('documento', []))

    return {
        "id": id,
        "tipificacaoPenal": tipificacao_penal,
        "endereco_1": endereco_1 if endereco_1 else '',
        "cpf": cpf
    }, None

# Lendo o arquivo JSON linha por linha
file_path = 'output/5.merged_respostas.json'
dados = []
erros = []

with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:  # Ignorar linhas vazias
            dado, erro = process_json_line(line)
            if dado:
                dados.append(dado)
            if erro:
                erros.append(erro)

# Criando os DataFrames
df_dados = pd.DataFrame(dados)
df_erros = pd.DataFrame(erros)

# Alterações nas colunas 'endereco_1' e 'tipificacaoPenal' do DataFrame df_dados
df_dados['endereco_1'] = df_dados['endereco_1'].str.replace('/None', '')
df_dados['endereco_1'] = df_dados['endereco_1'].str.replace('None,', '')
df_dados['endereco_1'] = df_dados['endereco_1'].str.replace(', None', '')
df_dados['endereco_1'] = df_dados['endereco_1'].str.replace(', ,', ',')
df_dados['endereco_1'] = df_dados['endereco_1'].str.upper()

def extract_first_tipificacao(tipificacoes):
    if tipificacoes:
        first_tipificacao = tipificacoes[0]
        return first_tipificacao.split(';')[0]
    return ''

df_dados['tipificacaoPenal'] = df_dados['tipificacaoPenal'].apply(lambda x: [extract_first_tipificacao(x)])

# Salvando em arquivos Excel separados
output_file_path_dados = 'output/4.dados_finais.xlsx'
output_file_path_erros = 'output/4.dados_erros.xlsx'
df_dados.to_excel(output_file_path_dados, index=False)
df_erros.to_excel(output_file_path_erros, index=False)

print('Processo Finalizado')
