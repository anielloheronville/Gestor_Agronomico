import pandas as pd
import numpy as np

# Metas de produtividade média fornecidas por você
target_averages = {
    'Milho': 5550,
    'Algodão': 1800,
    'Soja': 3970
}

file_path = 'ml_dataset_produtividade.csv'

try:
    df = pd.read_csv(file_path)
    print("Valores médios ANTES do ajuste:")
    print(df.groupby('cultura')['produtividade_kg_ha'].mean().round(0))
    print("-" * 40)

    # Cria uma cópia para trabalhar
    df_adjusted = df.copy()

    for culture, target_mean in target_averages.items():
        # Máscara para selecionar apenas a cultura atual
        culture_mask = df_adjusted['cultura'] == culture
        
        # Verifica se a cultura existe no DataFrame
        if not culture_mask.any():
            print(f"Aviso: Nenhuma linha encontrada para a cultura '{culture}'.")
            continue

        # Calcula a média atual para a cultura
        current_mean = df_adjusted.loc[culture_mask, 'produtividade_kg_ha'].mean()

        # Calcula e aplica o fator de ajuste
        if current_mean > 0:
            adjustment_factor = target_mean / current_mean
            print(f"Ajustando '{culture}': Fator de correção = {adjustment_factor:.4f}")
            df_adjusted.loc[culture_mask, 'produtividade_kg_ha'] *= adjustment_factor
        else:
            print(f"Aviso: Média atual para '{culture}' é 0. Não é possível ajustar.")

    # Salva o DataFrame atualizado, sobrescrevendo o arquivo original
    df_adjusted.to_csv(file_path, index=False)
    print(f"\nArquivo '{file_path}' foi atualizado e salvo.")
    print("-" * 40)

    # Etapa de verificação para confirmar as novas médias
    print("Verificação: Novos valores médios APÓS o ajuste:")
    df_verificacao = pd.read_csv(file_path)
    print(df_verificacao.groupby('cultura')['produtividade_kg_ha'].mean().round(0))
    print("\nProcesso de correção concluído com sucesso!")

except FileNotFoundError:
    print(f"ERRO: O arquivo '{file_path}' não foi encontrado.")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")