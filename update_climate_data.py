import requests
import pandas as pd
from datetime import date
from tqdm import tqdm
import numpy as np
import time

# =============================================================================
# 0. CONFIGURAÇÕES
# =============================================================================

# Coordenadas para a região de Sinop, MT
LATITUDE = -11.86
LONGITUDE = -55.49

# Período para buscar os dados históricos
ANO_INICIAL = 2004
ANO_FINAL = date.today().year

OUTPUT_CSV_FILE = "Dados_Climaticos_OPEN-METEO.csv"

# =============================================================================
# 1. FUNÇÃO PARA BUSCAR E PROCESSAR OS DADOS DA OPEN-METEO
# =============================================================================

def fetch_open_meteo_data(lat, lon, start_date, end_date):
    """Busca dados da API Open-Meteo para um período e local específicos."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation",
        "timezone": "auto"
    }
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return None

def process_and_save_data():
    """
    Orquestra a busca de dados anuais da Open-Meteo, processa-os e salva em um CSV.
    """
    all_data_df = pd.DataFrame()
    
    print(f"Iniciando a busca de dados da Open-Meteo para a região de Sinop, MT ({ANO_INICIAL}-{ANO_FINAL}).")
    print("Este processo pode levar alguns minutos...")
    
    for ano in tqdm(range(ANO_INICIAL, ANO_FINAL + 1), desc="Buscando dados anuais"):
        data_inicio_str = f"{ano}-01-01"
        
        if ano == ANO_FINAL:
            data_fim_str = date.today().strftime('%Y-%m-%d')
        else:
            data_fim_str = f"{ano}-12-31"
            
        dados_anuais = fetch_open_meteo_data(LATITUDE, LONGITUDE, data_inicio_str, data_fim_str)
        
        if dados_anuais and 'hourly' in dados_anuais:
            df_ano = pd.DataFrame(dados_anuais['hourly'])
            all_data_df = pd.concat([all_data_df, df_ano], ignore_index=True)
        
        time.sleep(1)

    if all_data_df.empty:
        print("\nNenhum dado foi baixado. O arquivo CSV não será criado.")
        return

    print(f"\nProcessando {len(all_data_df)} registros horários baixados...")
    
    # --- Padronização dos dados ---
    all_data_df.rename(columns={
        'time': 'DATETIME',
        'precipitation': 'PRECIPITACAO_TOTAL_HORARIO_mm',
        'temperature_2m': 'TEMPERATURA_AR_BULBO_SECO_HORARIA_C'
    }, inplace=True)
    
    all_data_df['DATETIME'] = pd.to_datetime(all_data_df['DATETIME'])
    
    all_data_df['PRECIPITACAO_TOTAL_HORARIO_mm'] = pd.to_numeric(all_data_df['PRECIPITACAO_TOTAL_HORARIO_mm'], errors='coerce')
    all_data_df['TEMPERATURA_AR_BULBO_SECO_HORARIA_C'] = pd.to_numeric(all_data_df['TEMPERATURA_AR_BULBO_SECO_HORARIA_C'], errors='coerce')
    
    # --- MUDANÇA: ANÁLISE E TRATAMENTO DE DADOS NULOS ---
    print("\nAnalisando a qualidade dos dados...")
    print(all_data_df.isnull().sum()) # Mostra quantos valores nulos existem por coluna
    
    # Usa interpolação linear para preencher pequenas falhas nos dados de temperatura
    all_data_df['TEMPERATURA_AR_BULBO_SECO_HORARIA_C'].interpolate(method='linear', inplace=True)
    # Para precipitação, é mais seguro preencher falhas com 0, pois a interpolação poderia criar chuva onde não houve
    all_data_df['PRECIPITACAO_TOTAL_HORARIO_mm'].fillna(0, inplace=True)
    
    print("\nDados nulos tratados com sucesso.")
    print(all_data_df.isnull().sum())
    
    df_final = all_data_df[['DATETIME', 'PRECIPITACAO_TOTAL_HORARIO_mm', 'TEMPERATURA_AR_BULBO_SECO_HORARIA_C']]
    
    df_final.to_csv(OUTPUT_CSV_FILE, index=False)
    
    print(f"\nSucesso! Arquivo '{OUTPUT_CSV_FILE}' foi criado com {len(df_final)} registros limpos e tratados.")

if __name__ == '__main__':
    process_and_save_data()