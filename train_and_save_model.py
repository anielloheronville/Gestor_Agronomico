import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib

def train_and_save():
    """
    Função completa para carregar os dados, treinar o modelo de pipeline
    e salvá-lo em um arquivo joblib.
    """
    try:
        # Carregar o dataset que preparamos
        df = pd.read_csv('ml_dataset_produtividade.csv')

        # Separar as features (X) e o alvo (y)
        X = df.drop('produtividade_kg_ha', axis=1)
        y = df['produtividade_kg_ha']

        # Identificar colunas categóricas para pré-processamento
        categorical_features = ['cultura', 'fase_enos']
        
        # Criar um transformador para aplicar One-Hot Encoding
        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ],
            remainder='passthrough'
        )

        # Criar o pipeline do modelo com o regressor
        model_pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                                         ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))])

        # Treinar o modelo com todos os dados disponíveis
        print("Treinando o modelo com todos os dados...")
        model_pipeline.fit(X, y)
        print("Treinamento concluído.")

        # Salvar o pipeline do modelo treinado
        joblib.dump(model_pipeline, 'yield_prediction_model.joblib')
        print("\nModelo salvo com sucesso como 'yield_prediction_model.joblib'")

    except FileNotFoundError:
        print("ERRO: O arquivo 'ml_dataset_produtividade.csv' não foi encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro durante o processo: {e}")

if __name__ == "__main__":
    train_and_save()