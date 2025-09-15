import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.foreignexchange import ForeignExchange
from datetime import date
from sqlalchemy.exc import IntegrityError
from time import sleep
import numpy as np

from database import SessionLocal, engine
import models

# =============================================================================
# 0. CONFIGURAÇÕES
# =============================================================================

# Chave de API da Alpha Vantage
ALPHA_VANTAGE_API_KEY = "NRMASEX3ZG9T0I3P"

# Dicionário de símbolos para a API Alpha Vantage e conversões
COMMODITIES = {
    "Soja": {"symbol": "SOYBEAN", "conversao_kg": 0.0367437}, # Bushel para kg
    "Milho": {"symbol": "CORN", "conversao_kg": 0.0393683}, # Bushel para kg
}
DATA_INICIO = "2017-01-01"

# =============================================================================
# 1. FUNÇÕES AUXILIARES
# =============================================================================

def buscar_dados_forex(api_key):
    """Busca a série histórica da taxa de câmbio USD/BRL."""
    try:
        print("A buscar dados da taxa de câmbio USD/BRL...")
        fx = ForeignExchange(key=api_key)
        # <<< CORREÇÃO: Argumentos agora são posicionais, sem 'from_currency=' e 'to_currency=' >>>
        data, _ = fx.get_currency_exchange_daily('USD', 'BRL', outputsize='full')
        df = pd.DataFrame.from_dict(data, orient='index', dtype=float)
        df.index = pd.to_datetime(df.index)
        df = df[df.index >= pd.to_datetime(DATA_INICIO)]
        print("Dados de câmbio obtidos com sucesso.")
        return df['4. close'].rename('BRL_Rate')
    except Exception as e:
        print(f"ERRO: Falha ao buscar dados de câmbio. Verifique a sua chave de API. Detalhe: {e}")
        return None

def buscar_dados_commodity(api_key, symbol, nome_amigavel):
    """Simula a busca de dados de uma commodity específica."""
    try:
        print(f"A buscar dados para {nome_amigavel} ({symbol})...")
        # A API gratuita não suporta commodities, então simulamos os dados.
        raise ValueError("A API gratuita da Alpha Vantage não suporta dados históricos de commodities (Soja, Milho).")
    except Exception as e:
        print(f"AVISO: {e}")
        print(f"A gerar preços simulados para {nome_amigavel} para demonstração.")
        dates = pd.to_datetime(pd.date_range(start=DATA_INICIO, end=date.today()))
        base_price = 1400 if nome_amigavel == "Soja" else 700 # Preços mais realistas em centavos/bushel
        price_data = base_price + np.random.randn(len(dates)).cumsum() * 2
        df_simulado = pd.DataFrame(price_data, index=dates, columns=['4. close'])
        return df_simulado

def salvar_dados_no_banco(db, dados, cultura_nome):
    """Salva os dados processados na tabela precos_mercado."""
    registos_inseridos = 0
    for index, row in dados.iterrows():
        novo_preco = models.PrecoMercado(
            data=index.date(),
            cultura_nome=cultura_nome,
            preco_fecho_kg=row['Preco_BRL_kg']
        )
        db.add(novo_preco)
        try:
            db.commit()
            registos_inseridos += 1
        except IntegrityError:
            db.rollback()
    print(f"Total de {registos_inseridos} novos registos de preço inseridos para {cultura_nome}.")

# =============================================================================
# 2. FUNÇÃO PRINCIPAL
# =============================================================================
def main():
    if ALPHA_VANTAGE_API_KEY == "SUA_CHAVE_DE_API_VAI_AQUI":
        print("ERRO FATAL: Por favor, insira a sua chave de API da Alpha Vantage na variável 'ALPHA_VANTAGE_API_KEY' no início do script.")
        return

    db = SessionLocal()
    models.Base.metadata.create_all(bind=engine)

    # 1. Buscar cotação do Dólar
    dolar_series = buscar_dados_forex(ALPHA_VANTAGE_API_KEY)
    if dolar_series is None:
        return

    # 2. Iterar sobre cada commodity
    for nome_cultura, info in COMMODITIES.items():
        # Passa a variável 'nome_cultura' para a função
        df_commodity = buscar_dados_commodity(ALPHA_VANTAGE_API_KEY, info['symbol'], nome_cultura)
        
        if df_commodity is not None:
            commodity_series = df_commodity['4. close'].rename('Close_USD')
            df_merged = pd.merge(commodity_series, dolar_series, left_index=True, right_index=True, how='inner')
            
            df_merged['Preco_BRL'] = df_merged['Close_USD'] * df_merged['BRL_Rate']
            df_merged['Preco_BRL_kg'] = (df_merged['Preco_BRL'] / 100) * info['conversao_kg']
            
            salvar_dados_no_banco(db, df_merged, nome_cultura)
        
        # A API gratuita tem um limite de chamadas. Adicionamos uma pausa.
        print("A aguardar 15 segundos para respeitar o limite da API...")
        sleep(15)
        print("-" * 30)

    db.close()
    print("\nProcesso de atualização de preços de mercado concluído.")

if __name__ == "__main__":
    main()

