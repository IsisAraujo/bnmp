import subprocess
import pandas as pd
import json
from openpyxl.workbook import Workbook

def normalizar_enderecos(enderecos):
    if isinstance(enderecos, list) and len(enderecos) > 0:
        endereco = enderecos[0]
        logradouro = endereco.get('logradouro', '')
        bairro = endereco.get('bairro', '')
        numero = endereco.get('numero', '')
        municipio = endereco.get('municipio', {}).get('nome', '')
        estado = endereco.get('estado', {}).get('sigla', '')
        return f"{logradouro}, {numero}, {bairro}, {municipio}/{estado}"
    return ''

def process_json_line(json_line):
    item = json.loads(json_line)
    id = item.get('id', '')
    response = item.get('response', {})
    
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

    return {
        "id": id,
        "tipificacaoPenal": tipificacao_penal,
        "endereco_1": endereco_1 if endereco_1 else ''
    }, None

file_path = 'output/3.todas_respostas.json'
dados = []
erros = []

with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            dado, erro = process_json_line(line)
            if dado:
                dados.append(dado)
            if erro:
                erros.append(erro)

df_dados = pd.DataFrame(dados)
df_erros = pd.DataFrame(erros)

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

output_file_path_erros = 'output/4.dados_erros.xlsx'  # Adicionada a extensão .xlsx
df_erros.to_excel(output_file_path_erros, index=False)

print('Processo Finalizado')

    # Executar o script de limpeza após a raspagem
print("Extraindo o Json dos arquivos com erros..")
subprocess.run(["python", "error_check_json.py"])

print('Processo Finalizado')

    # mesclando Json
print("Mesclando Json")
subprocess.run(["python", "mescla_json.py"])

print('Processo Finalizado')

#criando o dataset para usar no api maps parra tirar lat long
print("Criando o dataset para usar no api maps..")
subprocess.run(["python", "clear.py"])

print('Processo Finalizado')