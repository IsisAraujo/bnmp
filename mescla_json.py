import json

# Caminhos dos arquivos de entrada
file_path_all_responses = 'output/3.todas_respostas.json'
file_path_error_responses = 'output/4.1dados_erros.json'

# Caminho do arquivo de saída
output_file_path = 'output/5.merged_respostas.json'

# Função para ler um arquivo JSON e retornar seus objetos JSON individuais
def read_individual_json_objects(file_path):
    data = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                try:
                    obj = json.loads(line)
                    data.append(obj)
                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar uma linha no arquivo {file_path}: {e}")
    except FileNotFoundError:
        print(f"Erro: O arquivo {file_path} não foi encontrado.")
    return data

# Lendo os arquivos JSON
all_responses = read_individual_json_objects(file_path_all_responses)
error_responses = read_individual_json_objects(file_path_error_responses)

# Mesclando os dados (adicionando o conteúdo do arquivo de erros ao final do arquivo de todas as respostas)
merged_responses = all_responses + error_responses

# Salvando o resultado no novo arquivo
with open(output_file_path, 'w', encoding='utf-8') as file:
    for obj in merged_responses:
        json.dump(obj, file, ensure_ascii=False)
        file.write('\n')  # Adiciona uma quebra de linha entre cada objeto

print('Mesclagem concluída e dados salvos em', output_file_path)