import pandas as pd
import json
import subprocess 
    # Executar o script de limpeza após a raspagem
    print("Executando script de limpeza...")
    subprocess.run(["python", "clear.py"])

    print('Processo Finalizado')
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


print('Processo Finalizado')


