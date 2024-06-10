import pandas as pd
import json

# Função para formatar o nome do município
def formatar_municipio(municipio):
    if municipio is None:
        return ''
    else:
        return f"{municipio['nome']}/SE"

# Lista para armazenar os dados que serão convertidos em um DataFrame
dados = []

# Abre o arquivo e lê linha por linha
with open('output/3.todas_respostas.txt', 'r') as f:
    # Carrega o conteúdo do arquivo como JSON
    data = f.read()
    # Divide o conteúdo do arquivo em linhas
    lines = data.strip().split('\n')
    # Itera sobre cada linha
    for line in lines:
        # Carrega a linha como um objeto JSON
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON: {e}")
            continue
        
        # Extrai os dados de interesse
        id = obj.get('id')
        tipificacao_penal = obj.get('tipificacaoPenal', [{}])[0].get('rotulo', '').split(',')[0]  # Pega apenas o código da tipificação penal

        # Extrai os endereços da pessoa
        enderecos = obj.get('pessoa', {}).get('enderecos', [])

        # Se não houver endereços, adiciona um dicionário vazio
        if not enderecos:
            enderecos = [{}]

        # Pega o primeiro endereço
        primeiro_endereco = enderecos[0]

        # Constrói o endereço 1
        endereco_1 = ", ".join(filter(None, [
            primeiro_endereco.get("logradouro", ''),
            primeiro_endereco.get("bairro", ''),
            str(primeiro_endereco.get("numero", '')),
            primeiro_endereco.get("complemento", ''),
            str(primeiro_endereco.get("cep", '')),
            formatar_municipio(primeiro_endereco.get("municipio"))
        ]))

        # Pega o segundo endereço, se existir
        if len(enderecos) > 1:
            segundo_endereco = enderecos[1]
            # Constrói o endereço 2
            endereco_2 = ", ".join(filter(None, [
                segundo_endereco.get("logradouro", ''),
                segundo_endereco.get("bairro", ''),
                str(segundo_endereco.get("numero", '')),
                segundo_endereco.get("complemento", ''),
                str(segundo_endereco.get("cep", '')),
                formatar_municipio(segundo_endereco.get("municipio"))
            ]))
        else:
            endereco_2 = None

        # Adiciona os dados ao DataFrame
        dados.append({
            "id": id,
            "tipificacaoPenal": tipificacao_penal,
            "endereco_1": endereco_1 if endereco_1 != '' else None,
            "endereco_2": endereco_2 if endereco_2 != '' else None
        })

# Cria o DataFrame a partir dos dados
df = pd.DataFrame(dados)

# Salva o DataFrame em um arquivo Excel
df.to_excel('output/resultado_final.xlsx', index=False)

# Lê o arquivo Excel salvo
df = pd.read_excel('output/4.resultado_final.xlsx')

# Remove as partes indesejadas nos endereços do DataFrame
df['endereco_1'] = df['endereco_1'].str.replace(', None/SE', '').str.replace(', None', '')
df['endereco_2'] = df['endereco_2'].str.replace(', None/SE', '').str.replace(', None', '')

# Salva o DataFrame atualizado em um novo arquivo Excel
df.to_excel('output/4.resultado_final', index=False)

print('Arquivo Excel criado com sucesso, com as partes indesejadas removidas.')

#implementar a api