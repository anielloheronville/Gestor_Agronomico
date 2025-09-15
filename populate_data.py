import random
from datetime import date, timedelta
from database import SessionLocal, engine
import models
import operations

# =============================================================================
# 0. CONFIGURAÇÕES DA GERAÇÃO DE DADOS
# =============================================================================
NUM_TALHOES = 80
ANO_INICIAL = 2017
NUM_ANOS_SIMULACAO = 8
CULTURAS_COMERCIAIS = ["Soja", "Milho", "Algodão"]
OPERADORES = ["Carlos Silva", "João Pereira", "Marcos Costa", "Lucas Martins", "Rafael Souza"]
PRECO_COMBUSTIVEL_LT = 5.80 # R$/L

# Tabela de preços de insumos
CUSTOS_OPERACIONAIS = {
    "insumos": {
        "Calcário Dolomítico": {"preco": 120, "unidade": "ton"},
        "NPK 02-20-20": {"preco": 4.5, "unidade": "kg"}, "NPK 10-20-20": {"preco": 4.8, "unidade": "kg"}, "NPK 08-28-16": {"preco": 5.2, "unidade": "kg"},
        "Glifosato + S-metolachlor": {"preco": 55, "unidade": "L"}, "Atrazina + Tembotriona": {"preco": 75, "unidade": "L"}, "Diuron + Trifluralina": {"preco": 82, "unidade": "L"},
        "Tiametoxam + Lambda-cialotrina": {"preco": 95, "unidade": "L"}, "Bifentrina + Clorantraniliprol": {"preco": 120, "unidade": "L"}, "Malationa + Espinetoram": {"preco": 110, "unidade": "L"}
    }
}
CULTURA_PARAMETROS = {
    "Soja": {"produtividade_range": (3300, 4200), "adubo_produto": "NPK 02-20-20", "adubo_qtd": (250, 400), "herbicida_produto": "Glifosato + S-metolachlor", "inseticida_produto": "Tiametoxam + Lambda-cialotrina", "preco_venda_simulado": 1.10},
    "Milho": {"produtividade_range": (6000, 11000), "adubo_produto": "NPK 10-20-20", "adubo_qtd": (400, 650), "herbicida_produto": "Atrazina + Tembotriona", "inseticida_produto": "Bifentrina + Clorantraniliprol", "preco_venda_simulado": 0.85},
    "Algodão": {"produtividade_range": (3800, 5000), "adubo_produto": "NPK 08-28-16", "adubo_qtd": (300, 500), "herbicida_produto": "Diuron + Trifluralina", "inseticida_produto": "Malationa + Espinetoram", "preco_venda_simulado": 8.50}
}

# =============================================================================
# 1. FUNÇÕES AUXILIARES
# =============================================================================
def limpar_banco_de_dados(db_session):
    """Apaga todos os dados das tabelas para um novo preenchimento."""
    print("Limpando a base de dados...")
    # Ordem inversa para respeitar chaves estrangeiras
    db_session.query(models.PrecoMercado).delete()
    db_session.query(models.ContratoVenda).delete()
    db_session.query(models.AnaliseSolo).delete()
    db_session.query(models.AtividadeAgricola).delete()
    db_session.query(models.Safra).delete()
    db_session.query(models.Talhao).delete()
    db_session.query(models.Fazenda).delete()
    db_session.query(models.Cultura).delete()
    db_session.query(models.Maquina).delete()
    db_session.commit()

def criar_dados_iniciais_completos(db):
    operations.criar_dados_iniciais(db)
    if db.query(models.Maquina).count() == 0:
        maquinas = [
            models.Maquina(nome="John Deere DB74", tipo="Plantadeira", custo_hora_operacao=150.0, consumo_combustivel_l_h=25.0),
            models.Maquina(nome="Case IH Axial-Flow 9250", tipo="Colheitadeira", custo_hora_operacao=220.0, consumo_combustivel_l_h=35.0),
            models.Maquina(nome="Stara Imperador 3.0", tipo="Pulverizador", custo_hora_operacao=110.0, consumo_combustivel_l_h=18.0),
        ]
        db.add_all(maquinas); db.commit()

def gerar_analise_solo(db, talhao, ano):
    analise = models.AnaliseSolo(talhao_id=talhao.id, data_analise=date(ano, 5, 15), ph=round(random.uniform(4.8, 6.2), 2), fosforo_ppm=round(random.uniform(5.0, 25.0), 2), potassio_ppm=round(random.uniform(40.0, 150.0), 2), materia_organica_percent=round(random.uniform(1.5, 3.5), 2))
    db.add(analise); db.commit()

def gerar_safra_sintetica(db, talhao, cultura_nome, start_date):
    safra = operations.registrar_plantio(db, talhao.id, cultura_nome, start_date + timedelta(days=random.randint(1, 45)))
    if not safra: return
    params = CULTURA_PARAMETROS[cultura_nome]
    plantadeira, pulverizador, colheitadeira = [db.query(models.Maquina).filter(models.Maquina.tipo == t).first() for t in ["Plantadeira", "Pulverizador", "Colheitadeira"]]
    rendimento_ha_h = {"Plantadeira": 8, "Pulverizador": 20, "Colheitadeira": 7}
    def calcular_custo_op_ha(maq):
        horas_op = 1 / rendimento_ha_h[maq.tipo]
        return (horas_op * maq.consumo_combustivel_l_h * PRECO_COMBUSTIVEL_LT) + (horas_op * maq.custo_hora_operacao)
    
    custo_op_pulv_ha = calcular_custo_op_ha(pulverizador)
    qtd_calcario = round(random.uniform(1.5, 3.0), 1)
    custo_total_correcao = (qtd_calcario * CUSTOS_OPERACIONAIS["insumos"]["Calcário Dolomítico"]["preco"]) + custo_op_pulv_ha
    operations.registrar_atividade(db, safra.id, "Correção de Solo", "Calcário Dolomítico", qtd_calcario, "ton", safra.data_plantio - timedelta(days=30), pulverizador.nome, custo_total_correcao, random.choice(OPERADORES))
    
    qtd_adubo = random.randint(*params["adubo_qtd"])
    custo_total_plantio = (qtd_adubo * CUSTOS_OPERACIONAIS["insumos"][params["adubo_produto"]]["preco"]) + calcular_custo_op_ha(plantadeira)
    operations.registrar_atividade(db, safra.id, "Adubação e Plantio", params["adubo_produto"], qtd_adubo, "kg", safra.data_plantio, plantadeira.nome, custo_total_plantio, random.choice(OPERADORES))
    
    qtd_herbicida = round(random.uniform(1.5, 3.0), 1)
    custo_total_herbicida = (qtd_herbicida * CUSTOS_OPERACIONAIS["insumos"][params["herbicida_produto"]]["preco"]) + custo_op_pulv_ha
    operations.registrar_atividade(db, safra.id, "Herbicida", params["herbicida_produto"], qtd_herbicida, "L", safra.data_plantio + timedelta(days=30), pulverizador.nome, custo_total_herbicida, random.choice(OPERADORES))
    
    qtd_inseticida = round(random.uniform(1.0, 2.0), 1)
    custo_total_inseticida = (qtd_inseticida * CUSTOS_OPERACIONAIS["insumos"][params["inseticida_produto"]]["preco"]) + custo_op_pulv_ha
    operations.registrar_atividade(db, safra.id, "Inseticida", params["inseticida_produto"], qtd_inseticida, "L", safra.data_plantio + timedelta(days=60), pulverizador.nome, custo_total_inseticida, random.choice(OPERADORES))
    
    data_colheita = safra.data_colheita_prevista + timedelta(days=random.randint(-5, 10))
    produtividade = int(random.randint(*params["produtividade_range"]) * (1 + random.uniform(-0.05, 0.05)))
    operations.registrar_colheita(db, safra.id, data_colheita, produtividade, colheitadeira.nome, calcular_custo_op_ha(colheitadeira), random.choice(OPERADORES))

def gerar_contratos_venda(db):
    """Gera contratos de venda sintéticos para todas as safras colhidas."""
    print("\n--- A gerar contratos de venda sintéticos ---")
    safras_colhidas = db.query(models.Safra).filter(models.Safra.produtividade_kg_ha.isnot(None)).all()
    for safra in safras_colhidas:
        producao_total = safra.produtividade_kg_ha * safra.talhao.area_ha
        qtd_vendida = producao_total * random.uniform(0.8, 0.95) # Vende entre 80-95% da produção
        preco_base = CULTURA_PARAMETROS[safra.cultura.nome]["preco_venda_simulado"]
        preco_venda = preco_base * (1 + random.uniform(-0.1, 0.1)) # Variação de +/- 10% no preço
        data_venda = safra.data_colheita_real + timedelta(days=random.randint(5, 60))
        
        contrato = models.ContratoVenda(
            safra_id=safra.id,
            data_venda=data_venda,
            quantidade_kg=qtd_vendida,
            preco_venda_kg=round(preco_venda, 2)
        )
        db.add(contrato)
    db.commit()
    print(f"{len(safras_colhidas)} contratos de venda gerados.")

# =============================================================================
# 2. FUNÇÃO PRINCIPAL
# =============================================================================
def main():
    db = SessionLocal()
    models.Base.metadata.create_all(bind=engine)
    limpar_banco_de_dados(db)
    criar_dados_iniciais_completos(db)
    
    print("\n--- A criar Fazendas e Talhões ---")
    fazendas = db.query(models.Fazenda).all()
    talhoes = [operations.adicionar_talhao(db, random.choice(fazendas).nome, f"{random.choice(fazendas).nome.split()[1][:3].upper()}-{i+1:03}", round(random.uniform(18.0, 22.0), 2)) for i in range(NUM_TALHOES)]
    print(f"{len(talhoes)} talhões criados com sucesso.")
    
    print("\n--- A gerar dados de Análise de Solo ---")
    for talhao in talhoes:
        for ano in range(ANO_INICIAL, ANO_INICIAL + NUM_ANOS_SIMULACAO, 2):
            gerar_analise_solo(db, talhao, ano)
    print("Análises de solo geradas com sucesso.")

    for i in range(NUM_ANOS_SIMULACAO):
        ano_corrente = ANO_INICIAL + i
        for season_info in [("B", date(ano_corrente, 10, 1)), ("A", date(ano_corrente + 1, 2, 15))]:
            season_label, start_date = season_info
            print(f"\n--- A gerar dados para a Safra {start_date.year}/{season_label} ---")
            for j, talhao in enumerate(talhoes):
                # Lógica de rotação (simplificada para o exemplo final)
                ultima_cultura = db.query(models.Cultura.nome).join(models.Safra).filter(models.Safra.talhao_id == talhao.id).order_by(models.Safra.data_plantio.desc()).first()
                culturas_possiveis = [c for c in CULTURAS_COMERCIAIS if c != (ultima_cultura[0] if ultima_cultura else None)]
                nova_cultura = random.choice(culturas_possiveis)
                gerar_safra_sintetica(db, talhao, nova_cultura, start_date)
            print(f"\nSafra {start_date.year}/{season_label} gerada com sucesso.")

    gerar_contratos_venda(db)
            
    db.close()
    print("\nProcesso de preenchimento de dados completo.")

if __name__ == "__main__":
    main()

