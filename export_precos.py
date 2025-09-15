import pandas as pd
from sqlalchemy import create_engine
import sys

def export_price_table_to_csv():
    """
    Conecta ao banco de dados SQLite e exporta a tabela 'precos_mercado' para um arquivo CSV.
    """
    table_to_export = 'precos_mercado'
    csv_filename = f"db_{table_to_export}.csv"
    
    try:
        engine = create_engine("sqlite:///gestao_agricola.db")
        print(f"Conectado ao banco de dados gestao_agricola.db")

        print(f"Exportando a tabela '{table_to_export}'...")
        df = pd.read_sql_table(table_to_export, engine)
        
        df.to_csv(csv_filename, index=False)
        print(f" -> Tabela '{table_to_export}' salva com sucesso como '{csv_filename}'")

    except Exception as e:
        print(f"Ocorreu um erro durante a exportação: {e}", file=sys.stderr)
        print(f"Certifique-se de que o arquivo 'gestao_agricola.db' e a tabela '{table_to_export}' existem.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    export_price_table_to_csv()