import pandas as pd
import numpy as np

try:
    # --- 1. Carregar os dados a partir dos arquivos CSV ---
    print("Lendo os arquivos CSV exportados...")
    df_safras = pd.read_csv('db_safras.csv')
    df_culturas = pd.read_csv('db_culturas.csv')
    df_talhoes = pd.read_csv('db_talhoes.csv')
    df_fazendas = pd.read_csv('db_fazendas.csv')
    df_solo_raw = pd.read_csv('db_analises_solo.csv')
    df_oni = pd.read_csv('oni_data.csv')
    df_clima = pd.read_csv('Dados_Climaticos_INMET.csv')
    print("Todos os arquivos CSV foram lidos com sucesso.")

    # --- 2. Juntar os dados para recriar o dataframe principal ---
    print("Juntando e preparando as tabelas de dados...")
    # Renomear colunas 'id' e 'nome' para evitar conflitos no merge
    df_safras.rename(columns={'id': 'safra_id'}, inplace=True)
    df_culturas.rename(columns={'id': 'cultura_id', 'nome': 'cultura'}, inplace=True)
    df_talhoes.rename(columns={'id': 'talhao_id', 'identificador': 'talhao'}, inplace=True)
    df_fazendas.rename(columns={'id': 'fazenda_id', 'nome': 'fazenda'}, inplace=True)

    # Merge das tabelas principais
    df_agricola = pd.merge(df_safras, df_culturas, on='cultura_id')
    df_agricola = pd.merge(df_agricola, df_talhoes, on='talhao_id')
    df_agricola = pd.merge(df_agricola, df_fazendas, on='fazenda_id')

    # Remover linhas onde a produtividade (nosso alvo) é nula
    df_agricola = df_agricola[df_agricola['produtividade_kg_ha'].notna()].copy()

    # Converter colunas de data para o formato datetime
    df_agricola['data_plantio'] = pd.to_datetime(df_agricola['data_plantio'])
    df_agricola['data_colheita_real'] = pd.to_datetime(df_agricola['data_colheita_real'])
    df_solo_raw['data_analise'] = pd.to_datetime(df_solo_raw['data_analise'])

    # --- 3. Adicionar dados de Solo e ENOS ---
    # Adicionar dados de solo usando merge_asof para pegar a análise mais recente ANTES do plantio
    df_agricola = pd.merge_asof(
        df_agricola.sort_values('data_plantio'),
        df_solo_raw.sort_values('data_analise'),
        left_on='data_plantio', right_on='data_analise',
        by='talhao_id', direction='backward'
    )

    # Adicionar dados de ENOS com base no mês/ano do plantio
    df_agricola['ano'] = df_agricola['data_plantio'].dt.year
    df_agricola['mes'] = df_agricola['data_plantio'].dt.month
    df_agricola = pd.merge(df_agricola, df_oni, on=['ano', 'mes'], how='left')
    df_agricola.drop(columns=['ano', 'mes'], inplace=True)

    # --- 4. Engenharia de Features Climáticas ---
    print("Iniciando engenharia de features climáticas (pode levar alguns instantes)...")
    df_clima.rename(columns={
        'PRECIPITACAO_TOTAL_HORARIO_mm': 'precipitacao_mm',
        'TEMPERATURA_AR_BULBO_SECO_HORARIA_C': 'temperatura_c'
    }, inplace=True)
    df_clima['DATETIME'] = pd.to_datetime(df_clima['DATETIME'])
    df_clima.set_index('DATETIME', inplace=True)

    clima_features = []
    for index, safra in df_agricola.iterrows():
        if pd.notna(safra['data_plantio']) and pd.notna(safra['data_colheita_real']):
            clima_ciclo = df_clima.loc[safra['data_plantio']:safra['data_colheita_real']]
            if not clima_ciclo.empty:
                precipitacao_total = clima_ciclo['precipitacao_mm'].sum()
                temperatura_media = clima_ciclo['temperatura_c'].mean()
                temperatura_max = clima_ciclo['temperatura_c'].max()
                dias_calor_extremo = (clima_ciclo['temperatura_c'] > 32).sum() / 24 # Horas divididas por 24
                
                clima_features.append({
                    'safra_id': safra['safra_id'],
                    'precipitacao_total_ciclo': precipitacao_total,
                    'temperatura_media_ciclo': temperatura_media,
                    'temperatura_max_ciclo': temperatura_max,
                    'dias_calor_extremo_ciclo': dias_calor_extremo
                })

    df_clima_features = pd.DataFrame(clima_features)

    # --- 5. Unificar e Salvar o Dataset Final ---
    print("Finalizando e salvando o dataset para ML...")
    df_ml = pd.merge(df_agricola, df_clima_features, on='safra_id', how='inner')

    colunas_finais = [
        'cultura', 'area_ha', 'ph', 'fosforo_ppm', 'potassio_ppm',
        'materia_organica_percent', 'fase_enos', 'precipitacao_total_ciclo',
        'temperatura_media_ciclo', 'temperatura_max_ciclo', 'dias_calor_extremo_ciclo',
        'produtividade_kg_ha'
    ]
    colunas_existentes = [col for col in colunas_finais if col in df_ml.columns]
    df_ml_final = df_ml[colunas_existentes].dropna()

    df_ml_final.to_csv('ml_dataset_produtividade.csv', index=False)

    print("\nDataset para Machine Learning criado com sucesso!")
    print("Arquivo salvo como: ml_dataset_produtividade.csv")
    print("\n5 primeiras linhas do dataset final:")
    print(df_ml_final.head())

except FileNotFoundError as e:
    print(f"\nERRO: Arquivo não encontrado: {e.filename}")
    print("Certifique-se de que todos os arquivos CSV ('db_...', 'oni_data.csv', 'Dados_Climaticos_INMET.csv') estão no diretório correto.")
except Exception as e:
    print(f"\nOcorreu um erro inesperado: {e}")