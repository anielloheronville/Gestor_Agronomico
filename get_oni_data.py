import pandas as pd

def classificar_fase_enos(oni_index):
    """Classifica a fase ENOS com base no índice ONI."""
    if oni_index >= 0.5:
        return 'El Nino'
    elif oni_index <= -0.5:
        return 'La Nina'
    else:
        return 'Neutro'

def fetch_and_process_oni_data():
    """
    Busca os dados históricos do ONI da NOAA, processa-os
    e salva em um arquivo CSV local.
    """
    url = "https://origin.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt"
    
    print(f"Buscando dados de El Niño/La Niña de: {url}")
    
    try:
        # Usando sep=r'\s+' para tratar o aviso e ler corretamente
        df = pd.read_csv(url, sep=r'\s+')
        print("Dados brutos carregados com sucesso.")
        
        # MUDANÇA: Adaptando ao novo formato com as colunas corretas
        # Renomeia as colunas que nos interessam
        df.rename(columns={'YR': 'ano', 'MON': 'mes', 'ANOM': 'oni_index'}, inplace=True)
        
        # A transformação 'melt' e o mapeamento de meses NÃO SÃO MAIS NECESSÁRIOS
        
        # Classifica cada registro como El Niño, La Niña ou Neutro
        df['fase_enos'] = df['oni_index'].apply(classificar_fase_enos)
        
        # Filtra para ter um histórico de mais de 20 anos (desde o ano 2000)
        df_final = df[df['ano'] >= 2000].copy()
        
        # Seleciona e ordena as colunas finais
        df_final = df_final[['ano', 'mes', 'oni_index', 'fase_enos']]
        df_final.sort_values(by=['ano', 'mes'], inplace=True)
        
        # Salva o arquivo CSV que será usado pelo dashboard
        output_filename = 'oni_data.csv'
        df_final.to_csv(output_filename, index=False)
        
        print(f"\nSucesso! Arquivo '{output_filename}' foi criado com {len(df_final)} registros de dados climáticos.")
        print("O arquivo contém o histórico de El Niño e La Niña desde o ano 2000.")
        
    except Exception as e:
        print(f"\nOcorreu um erro ao buscar ou processar os dados: {e}")
        print("Verifique sua conexão com a internet ou se a URL da NOAA está acessível.")

if __name__ == '__main__':
    fetch_and_process_oni_data()