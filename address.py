import pandas as pd
import googlemaps
from tqdm import tqdm

# Função para geocodificar um único endereço
def geocode_address(address, gmaps):
    if pd.isna(address) or address == '':
        return None, None
    
    try:
        geocode_result = gmaps.geocode(address)
        if (geocode_result) and (len(geocode_result) > 0):
            location = geocode_result[0]['geometry']['location']
            return location['lat'], location['lng']
        else:
            return None, None
    except Exception as e:
        print(f"Erro na geocodificação do endereço '{address}': {str(e)}")
        return None, None

# Lendo o arquivo Excel com os dados limpos
input_file_path = 'output/4.dados_finais.xlsx'
df = pd.read_excel(input_file_path)

# Inicializando o cliente Google Maps
gmaps = googlemaps.Client(key='*')  # Substitua com sua chave de API

# Adicionando a barra de progresso
latitudes = []
longitudes = []

for address in tqdm(df['endereco_1'], desc="Geocodificando endereços"):
    lat, lng = geocode_address(address, gmaps)
    latitudes.append(lat)
    longitudes.append(lng)

# Atribuindo os resultados ao DataFrame
df['lat'] = latitudes
df['lng'] = longitudes

# Salvando o DataFrame atualizado com as colunas de latitude e longitude
output_file_path = 'output/5.1.dados_geocodificados.xlsx'
df.to_excel(output_file_path, index=False)

print('Geocodificação concluída e dados salvos em', output_file_path)

# Carregar os DataFrames dos arquivos
file_dados_geocodificados = 'output/5.1dados_geocodificados.xlsx'
file_dados_gerais = 'output/2.dados_gerais.xlsx'

df_geocodificados = pd.read_excel(file_dados_geocodificados)
df_gerais = pd.read_excel(file_dados_gerais)

# Realizar o merge pelos IDs
df_merged = pd.merge(df_gerais, df_geocodificados, on='id', how='left')

# Salvando o DataFrame resultante em um novo arquivo Excel
output_merged_file = 'output/6.mandados_com_endereco.xlsx'
df_merged.to_excel(output_merged_file, index=False)

print('Merge realizado com sucesso.')