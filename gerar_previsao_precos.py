import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import logging
import os

# Opcional: Suprime mensagens informativas do Prophet para um output mais limpo
logging.getLogger('cmdstanpy').setLevel(logging.WARNING)

def prever_precos_de_mercado():
    """
    Carrega os dados históricos de preços, treina um modelo Prophet para cada cultura,
    gera uma previsão para os próximos 6 meses e salva os resultados.
    """
    forecast_file = 'previsao_precos_mercado.csv'

    # Se a previsão já existe, pula o treinamento e vai direto para o gráfico
    if not os.path.exists(forecast_file):
        try:
            df_prices = pd.read_csv('db_precos_mercado.csv')
            df_prices['data'] = pd.to_datetime(df_prices['data'])
            print("Arquivo 'db_precos_mercado.csv' carregado com sucesso.")
        except FileNotFoundError:
            print("ERRO: O arquivo 'db_precos_mercado.csv' não foi encontrado.")
            return

        culturas = df_prices['cultura_nome'].unique()
        all_forecasts = []

        print("\nIniciando treinamento e previsão para cada cultura...")

        for cultura in culturas:
            print(f"- Processando {cultura}...")
            
            df_cultura = df_prices[df_prices['cultura_nome'] == cultura][['data', 'preco_fecho_kg']].copy()
            df_cultura.rename(columns={'data': 'ds', 'preco_fecho_kg': 'y'}, inplace=True)
            df_cultura = df_cultura.sort_values(by='ds').drop_duplicates(subset=['ds'], keep='last')
            
            if len(df_cultura) < 10:
                print(f"  Aviso: Dados insuficientes para prever '{cultura}'. Pulando.")
                continue
                
            model = Prophet(yearly_seasonality=True, daily_seasonality=False)
            model.fit(df_cultura)
            
            future = model.make_future_dataframe(periods=182)
            forecast = model.predict(future)
            
            forecast['cultura_nome'] = cultura
            all_forecasts.append(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'cultura_nome']])

        if not all_forecasts:
            print("\nNenhuma previsão foi gerada devido à falta de dados.")
            return
            
        df_forecast_final = pd.concat(all_forecasts)
        df_forecast_final.to_csv(forecast_file, index=False)
        print(f"\nPrevisão para todas as culturas salva com sucesso em '{forecast_file}'")
    else:
        print(f"Arquivo de previsão '{forecast_file}' já existe. Pulando treinamento.")

    # Etapa de Visualização
    print("Gerando gráfico de exemplo para a Soja...")
    
    # Carrega os dois arquivos necessários para o gráfico
    df_prices = pd.read_csv('db_precos_mercado.csv')
    df_prices['data'] = pd.to_datetime(df_prices['data'])
    
    df_forecast_final = pd.read_csv(forecast_file)
    df_forecast_final['ds'] = pd.to_datetime(df_forecast_final['ds'])
    
    df_soja_hist = df_prices[df_prices['cultura_nome'] == 'Soja']
    df_soja_fcst = df_forecast_final[df_forecast_final['cultura_nome'] == 'Soja']

    plt.figure(figsize=(12, 7))
    # LINHA CORRIGIDA: Usa 'data' e 'preco_fecho_kg' para os dados históricos
    plt.plot(df_soja_hist['data'], df_soja_hist['preco_fecho_kg'], 'k.', label='Dados Históricos')
    
    # Linhas para a previsão (já usavam os nomes corretos)
    plt.plot(df_soja_fcst['ds'], df_soja_fcst['yhat'], ls='--', c='blue', label='Previsão')
    plt.fill_between(df_soja_fcst['ds'], df_soja_fcst['yhat_lower'], df_soja_fcst['yhat_upper'], color='blue', alpha=0.2, label='Intervalo de Confiança (80%)')
    
    plt.title('Previsão de Preços para Soja (Próximos 6 Meses)')
    plt.xlabel('Data')
    plt.ylabel('Preço (R$/kg)')
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.legend()
    
    plt.savefig('previsao_soja_plot.png')
    print("Gráfico de exemplo 'previsao_soja_plot.png' salvo com sucesso.")


if __name__ == "__main__":
    prever_precos_de_mercado()