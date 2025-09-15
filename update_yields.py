import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import sys

def update_database_yields():
    """
    Ajusta os valores de produtividade no banco de dados para corresponderem
    a novas médias, mantendo a variabilidade relativa.
    """
    # Valores de produtividade média que você forneceu
    target_averages = {
        'Milho': 5550,
        'Algodão': 1800,
        'Soja': 3970
    }

    try:
        engine = create_engine("sqlite:///gestao_agricola.db")

        # Ler as tabelas necessárias
        df_safras = pd.read_sql_table('safras', engine)
        df_culturas = pd.read_sql_table('culturas', engine)
        
        # Juntar para ter o nome da cultura junto com a safra
        df_merged = pd.merge(df_safras, df_culturas, left_on='cultura_id', right_on='id', suffixes=('_safra', '_cultura'))

        print("Iniciando a atualização do banco de dados...")
        
        # Usar uma transação para garantir que todas as atualizações ocorram ou nenhuma
        with engine.begin() as connection:
            for cultura_nome, target_avg in target_averages.items():
                
                # Filtra os dados para a cultura atual
                cultura_data = df_merged[df_merged['nome'] == cultura_nome]
                if cultura_data.empty:
                    print(f"- Nenhuma safra encontrada para '{cultura_nome}'. Pulando.")
                    continue
                
                # Calcula a média atual
                current_avg = cultura_data['produtividade_kg_ha'].mean()

                if pd.isna(current_avg) or current_avg == 0:
                    print(f"- Média atual para '{cultura_nome}' é 0 ou inválida. Pulando.")
                    continue
                
                # Calcula o fator de ajuste
                adjustment_factor = target_avg / current_avg
                print(f"- Ajustando '{cultura_nome}': Média atual={current_avg:,.0f} kg/ha, Fator={adjustment_factor:.2f}")

                # Itera em cada safra da cultura para aplicar o ajuste
                for index, row in cultura_data.iterrows():
                    safra_id = row['id_safra']
                    current_yield = row['produtividade_kg_ha']
                    
                    # Aplica o fator de ajuste
                    new_yield = current_yield * adjustment_factor
                    
                    # Adiciona uma pequena variação aleatória (+/- 5%) para realismo
                    random_factor = np.random.uniform(0.95, 1.05)
                    final_yield = new_yield * random_factor
                    
                    # Prepara e executa o comando SQL de atualização
                    stmt = text("UPDATE safras SET produtividade_kg_ha = :yield WHERE id = :id")
                    connection.execute(stmt, {"yield": final_yield, "id": safra_id})
        
        print("\nBanco de dados 'gestao_agricola.db' foi atualizado com sucesso!")

    except Exception as e:
        print(f"\nOcorreu um erro: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    update_database_yields()