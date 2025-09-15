# projeto_agricola/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Define o caminho do arquivo do banco de dados SQLite
DATABASE_URL = "sqlite:///gestao_agricola.db"

# Cria a engine do banco de dados
engine = create_engine(DATABASE_URL)

# Cria uma sessão para interagir com o banco de dados
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para as classes de modelo (tabelas)
Base = declarative_base()