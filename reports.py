# projeto_agricola/reports.py
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import engine

def gerar_relatorio_produtividade(db_engine):
    """Gera um relatório de produtividade por cultura e fazenda."""
    query = """
    SELECT
        f.nome as fazenda,
        c.nome as cultura,
        t.identificador as talhao,
        s.produtividade_kg_ha,
        s.data_colheita_real
    FROM safras s
    JOIN culturas c ON s.cultura_id = c.id
    JOIN talhoes t ON s.talhao_id = t.id
    JOIN fazendas f ON t.fazenda_id = f.id
    WHERE s.produtividade_kg_ha IS NOT NULL
    ORDER BY f.nome, c.nome;
    """
    
    df = pd.read_sql_query(text(query), db_engine)
    
    if df.empty:
        print("Nenhum dado de colheita para gerar relatório.")
        return
        
    print("\n--- Relatório de Produtividade (kg/ha) ---")
    print(df)
    
    # Agrupar dados
    produtividade_media_cultura = df.groupby('cultura')['produtividade_kg_ha'].mean().round(2)
    print("\n--- Produtividade Média por Cultura ---")
    print(produtividade_media_cultura)
    
    produtividade_media_fazenda = df.groupby('fazenda')['produtividade_kg_ha'].mean().round(2)
    print("\n--- Produtividade Média por Fazenda ---")
    print(produtividade_media_fazenda)