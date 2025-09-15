from sqlalchemy import (Column, Integer, String, Float, Date, ForeignKey, 
                        UniqueConstraint)
from sqlalchemy.orm import relationship
from database import Base

class Fazenda(Base):
    __tablename__ = "fazendas"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, unique=True, index=True)
    localizacao = Column(String)
    talhoes = relationship("Talhao", back_populates="fazenda")

class Talhao(Base):
    __tablename__ = "talhoes"
    id = Column(Integer, primary_key=True, index=True)
    fazenda_id = Column(Integer, ForeignKey("fazendas.id"))
    identificador = Column(String, unique=True, index=True)
    area_ha = Column(Float)
    fazenda = relationship("Fazenda", back_populates="talhoes")
    safras = relationship("Safra", back_populates="talhao")
    analises_solo = relationship("AnaliseSolo", back_populates="talhao")

class Cultura(Base):
    __tablename__ = "culturas"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, unique=True)
    tipo = Column(String)
    ciclo_fisiologico_dias = Column(Integer)

class Maquina(Base):
    __tablename__ = "maquinas"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String)
    tipo = Column(String)
    custo_hora_operacao = Column(Float)
    consumo_combustivel_l_h = Column(Float)

class Safra(Base):
    __tablename__ = "safras"
    id = Column(Integer, primary_key=True, index=True)
    talhao_id = Column(Integer, ForeignKey("talhoes.id"))
    cultura_id = Column(Integer, ForeignKey("culturas.id"))
    data_plantio = Column(Date)
    data_colheita_prevista = Column(Date)
    data_colheita_real = Column(Date, nullable=True)
    produtividade_kg_ha = Column(Float, nullable=True)
    talhao = relationship("Talhao", back_populates="safras")
    cultura = relationship("Cultura")
    atividades = relationship("AtividadeAgricola", back_populates="safra")
    contratos_venda = relationship("ContratoVenda", back_populates="safra")

class AtividadeAgricola(Base):
    __tablename__ = "atividades_agricolas"
    id = Column(Integer, primary_key=True, index=True)
    safra_id = Column(Integer, ForeignKey("safras.id"))
    tipo_atividade = Column(String)
    produto_utilizado = Column(String, nullable=True)
    quantidade_aplicada_ha = Column(Float)
    unidade = Column(String)
    data_execucao = Column(Date)
    maquina_id = Column(Integer, ForeignKey("maquinas.id"), nullable=True)
    operador = Column(String, nullable=True)
    custo_total_ha = Column(Float)
    safra = relationship("Safra", back_populates="atividades")
    maquina = relationship("Maquina")

class AnaliseSolo(Base):
    __tablename__ = "analises_solo"
    id = Column(Integer, primary_key=True, index=True)
    talhao_id = Column(Integer, ForeignKey("talhoes.id"))
    data_analise = Column(Date)
    ph = Column(Float)
    fosforo_ppm = Column(Float)
    potassio_ppm = Column(Float)
    materia_organica_percent = Column(Float)
    talhao = relationship("Talhao", back_populates="analises_solo")

class ContratoVenda(Base):
    __tablename__ = "contratos_venda"
    id = Column(Integer, primary_key=True, index=True)
    safra_id = Column(Integer, ForeignKey("safras.id"))
    data_venda = Column(Date)
    quantidade_kg = Column(Float)
    preco_venda_kg = Column(Float)
    safra = relationship("Safra", back_populates="contratos_venda")

class PrecoMercado(Base):
    __tablename__ = "precos_mercado"
    id = Column(Integer, primary_key=True, index=True)
    data = Column(Date)
    cultura_nome = Column(String)
    preco_fecho_kg = Column(Float)
    __table_args__ = (UniqueConstraint('data', 'cultura_nome', name='_data_cultura_uc'),)

