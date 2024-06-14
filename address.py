pip install googlemaps
import pandas as pd
import googlemaps

# Função para geocodificar um único endereço
def geocode_address(address, gmaps):
    if pd.isna(address) or address == '':
        return None, None
    
    try:
        geocode_result = gmaps.geocode(address)
        if geocode_result:
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
gmaps = googlemaps.Client(key='SUA_CHAVE_DE_API_AQUI')  # Substitua com sua chave de API

# Aplicando a geocodificação para cada endereço na coluna 'endereco'
df['lat'], df['lng'] = zip(*df['endereco'].apply(lambda x: geocode_address(x, gmaps)))

# Salvando o DataFrame atualizado com as colunas de latitude e longitude
output_file_path = 'output/5.dados_geocodificados.xlsx'
df.to_excel(output_file_path, index=False)

print('Geocodificação concluída e dados salvos em', output_file_path)
