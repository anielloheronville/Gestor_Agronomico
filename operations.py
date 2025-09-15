from datetime import date, timedelta
from sqlalchemy.orm import Session
import models

def criar_dados_iniciais(db: Session):
    """Cria dados básicos de culturas e fazendas, se não existirem."""
    if db.query(models.Cultura).count() == 0:
        culturas = [
            models.Cultura(nome="Soja", tipo="Comercial", ciclo_fisiologico_dias=120),
            models.Cultura(nome="Milho", tipo="Comercial", ciclo_fisiologico_dias=150),
            models.Cultura(nome="Algodão", tipo="Comercial", ciclo_fisiologico_dias=180),
            models.Cultura(nome="Sorgo", tipo="Cobertura", ciclo_fisiologico_dias=90),
            models.Cultura(nome="Braquiária", tipo="Cobertura", ciclo_fisiologico_dias=75),
        ]
        db.add_all(culturas)
        
    if db.query(models.Fazenda).count() == 0:
        fazendas = [
            models.Fazenda(nome="Fazenda Cristalina", localizacao="GO"),
            models.Fazenda(nome="Fazenda Boa Esperança", localizacao="MT"),
            models.Fazenda(nome="Fazenda Alvorada", localizacao="BA"),
        ]
        db.add_all(fazendas)
    db.commit()

def adicionar_talhao(db: Session, fazenda_nome: str, identificador: str, area_ha: float):
    """Adiciona um novo talhão a uma fazenda existente."""
    fazenda = db.query(models.Fazenda).filter(models.Fazenda.nome == fazenda_nome).first()
    if not fazenda:
        print(f"Erro: Fazenda '{fazenda_nome}' não encontrada.")
        return None
    
    novo_talhao = models.Talhao(identificador=identificador, area_ha=area_ha, fazenda_id=fazenda.id)
    db.add(novo_talhao)
    db.commit()
    db.refresh(novo_talhao)
    return novo_talhao

def registrar_plantio(db: Session, talhao_id: int, cultura_nome: str, data_plantio: date):
    """Regista o início de uma nova safra num talhão."""
    cultura = db.query(models.Cultura).filter(models.Cultura.nome == cultura_nome).first()
    talhao = db.query(models.Talhao).get(talhao_id)
    if not cultura or not talhao:
        print("Erro: Cultura ou Talhão não encontrado.")
        return None

    data_colheita_prevista = data_plantio + timedelta(days=cultura.ciclo_fisiologico_dias)
    nova_safra = models.Safra(
        talhao_id=talhao_id,
        cultura_id=cultura.id,
        data_plantio=data_plantio,
        data_colheita_prevista=data_colheita_prevista
    )
    db.add(nova_safra)
    db.commit()
    db.refresh(nova_safra)
    return nova_safra

def registrar_atividade(db: Session, safra_id: int, tipo: str, produto: str, qtd: float, unidade: str, data: date, maquina_nome: str = None, custo_ha: float = 0.0, operador: str = None):
    """Regista uma atividade agrícola, incluindo os seus custos e operador."""
    safra = db.query(models.Safra).get(safra_id)
    if not safra:
        print("Erro: Safra não encontrada.")
        return None
        
    maquina_id = None
    if maquina_nome:
        maquina = db.query(models.Maquina).filter(models.Maquina.nome == maquina_nome).first()
        if maquina:
            maquina_id = maquina.id

    nova_atividade = models.AtividadeAgricola(
        safra_id=safra_id,
        tipo_atividade=tipo,
        produto_utilizado=produto,
        quantidade_aplicada_ha=qtd,
        unidade=unidade,
        data_execucao=data,
        maquina_id=maquina_id,
        custo_total_ha=custo_ha,
        operador=operador
    )
    db.add(nova_atividade)
    db.commit()

def registrar_colheita(db: Session, safra_id: int, data_colheita: date, produtividade: float, maquina_nome: str, custo_operacional_ha: float = 0.0, operador: str = None):
    """Regista os dados finais de colheita de uma safra."""
    safra = db.query(models.Safra).get(safra_id)
    if not safra:
        print("Erro: Safra não encontrada.")
        return

    safra.data_colheita_real = data_colheita
    safra.produtividade_kg_ha = produtividade
    
    # Regista a colheita como uma atividade também, para consolidar custos e operadores
    registrar_atividade(db, safra_id, 'Colheita', 'N/A', produtividade, 'kg/ha', data_colheita, maquina_nome, custo_operacional_ha, operador)
    
    db.commit()

