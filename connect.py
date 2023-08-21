from typing import Optional
from decouple import config
# from sqlmodel import Field, Session, SQLModel, create_engine
from icecream import ic
# from sqlalchemy import create_engine
# from sqlalchemy.engine import URL
# from sqlalchemy.exc import SQLAlchemyError
#
postgres_username = config('POSTGRES_USERNAME')
postgres_host = config('POSTGRES_HOST')
postgres_password = config('POSTGRES_PASSWORD')
postgres_port = config('POSTGRES_PORT')
postgres_database = 'data_warehouse'
#
# from sqlalchemy import create_engine
# from sqlalchemy.engine import URL
#
#
# url = URL.create(
#     drivername="postgresql",
#     username=postgres_username,
#     host=postgres_host,
#     database=postgres_database,
#     password=postgres_password
# )
#
# try:
#     engine = create_engine(url)
#     connection = engine.connect()
#
#     # Executa uma consulta para verificar a versão do PostgreSQL
#     result = connection.execute("SELECT version()")
#     print("Conexão bem-sucedida!")
#     print("Versão do PostgreSQL:", result.fetchone()[0])
#
#     # Fecha a conexão
#     connection.close()
#
# except SQLAlchemyError as e:
#     print("Erro ao conectar ou executar consulta:", e)
#
# #
# ic(
# postgres_username,
# postgres_host,
# postgres_password,
# postgres_port)

#
#
# class Movies(SQLModel, table=True):
#     id: Optional[int] = Field(default=None, primary_key=True)
#     name: str
#     genre: str
#     sensitive: Optional[bool] = False
#
#
# movie_1 = Movies(name="Fight Club", genre="Thriller",sensitive=True)
#
# engine = create_engine(f"postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}")
#
#
# SQLModel.metadata.create_all(engine)
#
# with Session(engine) as session:
#     session.add(movie_1)
#     session.commit()





import psycopg2

try:
    # Estabelece a conexão com o banco de dados
    conn = psycopg2.connect(
        dbname=postgres_database,
        user=postgres_username,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port
    )

    # Cria um cursor para executar comandos SQL
    cursor = conn.cursor()

    # Executa um comando SQL de teste
    cursor.execute('SELECT version()')

    # Obtém o resultado da consulta
    versao = cursor.fetchone()

    # Imprime o resultado
    print("Conexão bem-sucedida!")
    print("Versão do PostgreSQL:", versao[0])

    # Fecha o cursor e a conexão
    cursor.close()
    conn.close()

except psycopg2.Error as e:
    print("Erro ao conectar ou executar consulta:", e)

