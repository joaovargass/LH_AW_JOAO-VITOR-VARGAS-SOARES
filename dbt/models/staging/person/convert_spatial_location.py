import struct
import pandas as pd
import requests

# Função para converter o valor hexadecimal em lat/long
def convert_hex_to_lat_lon(hex_data):
    try:
        if not hex_data.startswith('0x'):
            hex_data = '0x' + hex_data

        binary_data = bytes.fromhex(hex_data[2:])
        longitude, latitude = struct.unpack('<dd', binary_data[-16:])

        return latitude, longitude

    except (ValueError, struct.error) as e:
        return None, None

# Função para consultar o ZIP code a partir da lat/long usando a API do Google
def get_zipcode_from_latlon(lat, lon, api_key):
    try:
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={api_key}"
        response = requests.get(url)
        response_data = response.json()

        if response_data['status'] == 'OK':
            for component in response_data['results'][0]['address_components']:
                if 'postal_code' in component['types']:
                    return component['long_name']
        return None

    except Exception as e:
        return None

def model(dbt, session):
    # Carregar a Google API Key do arquivo de texto
    with open('keys/google_api_key.txt', 'r') as file:
        google_api_key = file.read().strip()

    # Carregar os dados da tabela stg_address
    df = dbt.ref("stg_address").to_pandas()

    # Converter spatial_location para latitude e longitude
    df[['latitude', 'longitude']] = df['spatial_location'].apply(lambda x: pd.Series(convert_hex_to_lat_lon(x)))

    # Remover a coluna original spatial_location
    df = df.drop(columns=['spatial_location'])

    # Corrigir os ZIP codes usando a API do Google
    df['postal_code'] = df.apply(lambda row: get_zipcode_from_latlon(row['latitude'], row['longitude'], google_api_key), axis=1)

    # Retornar o DataFrame atualizado
    return df
