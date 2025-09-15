import pandas as pd
from sqlalchemy import create_engine
import sys

def export_tables_to_csv():
    """
    Conecta ao banco de dados SQLite e exporta tabelas específicas para arquivos CSV.
    """
    try:
        engine = create_engine("sqlite:///gestao_agricola.db")
        print("Conectado ao banco de dados gestao_agricola.db")

        tabelas_para_exportar = ['fazendas', 'talhoes', 'safras', 'culturas', 'analises_solo']

        for nome_tabela in tabelas_para_exportar:
            print(f"Exportando tabela '{nome_tabela}'...")
            df = pd.read_sql_table(nome_tabela, engine)
            nome_arquivo_csv = f"db_{nome_tabela}.csv"
            df.to_csv(nome_arquivo_csv, index=False)
            print(f" -> Tabela '{nome_tabela}' salva como '{nome_arquivo_csv}'")

        print("\nExportação de todas as tabelas concluída com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro durante a exportação: {e}", file=sys.stderr)
        print("Certifique-se de que o arquivo 'gestao_agricola.db' está no mesmo diretório que este script.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    export_tables_to_csv()