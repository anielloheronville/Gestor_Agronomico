# projeto_agricola/main.py
from datetime import date
from database import SessionLocal, engine
import models
import operations
import reports

def main():
    # Cria as tabelas no banco de dados
    models.Base.metadata.create_all(bind=engine)
    
    # Obtém uma sessão do banco
    db = SessionLocal()
    
    # 1. SETUP INICIAL
    print("--- 1. Configurando dados iniciais ---")
    operations.criar_dados_iniciais(db)
    
    # Adicionando um talhão para demonstração
    operations.adicionar_talhao(db, "Fazenda Boa Esperança", "MT-001", 20.0)
    talhao_demo = db.query(models.Talhao).filter(models.Talhao.identificador == "MT-001").first()
    
    print("\n--- 2. Simulando uma Safra de Soja ---")
    # 2. REGISTRAR PLANTIO
    safra_soja = operations.registrar_plantio(db, talhao_demo.id, "Soja", date(2024, 10, 15))
    
    # 3. REGISTRAR ATIVIDADES
    if safra_soja:
        operations.registrar_atividade(db, safra_soja.id, "Correção de Solo", "Calcário Dolomítico", 2.0, "ton", date(2024, 9, 1))
        operations.registrar_atividade(db, safra_soja.id, "Adubação NPK", "NPK 04-14-08", 350, "kg", date(2024, 10, 15), "John Deere DB74")
        operations.registrar_atividade(db, safra_soja.id, "Herbicida", "Glifosato", 2.5, "L", date(2024, 11, 20), "Stara Imperador 3.0")
    
    # 4. REGISTRAR COLHEITA
    print("\n--- 3. Registrando Colheita ---")
    if safra_soja:
        # Produtividade em kg/ha. Ex: 60 sacas/ha * 60 kg/saca = 3600 kg/ha
        operations.registrar_colheita(db, safra_soja.id, date(2025, 2, 20), 3600.0, "Case IH Axial-Flow 9250")

    # 5. GERAR RELATÓRIO
    print("\n--- 4. Gerando Relatórios ---")
    reports.gerar_relatorio_produtividade(engine)

    # 6. SUGESTÃO DE ROTAÇÃO
    print("\n--- 5. Sugestão para a Próxima Safra (Rotação de Cultura) ---")
    operations.sugerir_rotacao(db, talhao_demo.id)
    
    # Simulando agora o plantio de cobertura (safrinha)
    print("\n--- 6. Simulando Safrinha com Braquiária ---")
    safra_cobertura = operations.registrar_plantio(db, talhao_demo.id, "Braquiária", date(2025, 3, 5))
    
    print("\n--- 7. Verificando sugestão após a cobertura ---")
    operations.sugerir_rotacao(db, talhao_demo.id)

    # Fechar a sessão
    db.close()

if __name__ == "__main__":
    main()