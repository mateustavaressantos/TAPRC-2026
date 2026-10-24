import csv
import io
import logging
import os
from datetime import UTC, datetime

import azure.functions as func
import pyodbc
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

logger = logging.getLogger(__name__)


# chamado
@app.timer_trigger(
    schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.chamado"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.chamado")

        # Cria um buffer de memória para armazenar os dados CSV
        buffer = io.StringIO()
        # Cria um objeto CSV writer e escreve os dados no buffer
        writer = csv.writer(buffer)
        # Escreve o cabeçalho e as linhas no buffer CSV
        writer.writerow(columns)
        # Escreve todas as linhas no buffer CSV
        writer.writerows(rows)

        # Grava o arquivo no contêiner bronze do Data Lake, particionado por data
        now = datetime.now(UTC)
        file_path = f"itsm/chamado/{now:%Y/%m/%d}/chamado_{now:%Y%m%d_%H%M%S}.csv"

        # itsm/chamado/year=2024/month=06/day=05/chamado_20240605_123456.csv
        # file_path = f"itsm/chamado/year={now:%Y}/month={now:%m}/day={now:%d}/chamado_{now:%Y%m%d_%H%M%S}.csv"

        # Cria o cliente do Data Lake e grava o arquivo no contêiner bronze
        datalake = DataLakeServiceClient.from_connection_string(
            os.getenv("DATALAKE_CONNECTION_STRING")
        )
        # Cria o cliente do sistema de arquivos e grava o arquivo no contêiner bronze
        file_client = datalake.get_file_system_client("bronze").get_file_client(
            file_path
        )
        # Grava os dados do buffer no arquivo do Data Lake, sobrescrevendo se já existir
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logger.info(f"Arquivo gravado em bronze/{file_path}")

    except Exception as e:
        logger.error(f"Erro ao ler itsm.chamado: {e!s}")
        raise


# Chamado_sla
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.chamado_sla")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logger.info("A consulta não retornou nenhum dado.")
            else:
                for row in rows:
                    logger.info(f"Registro encontrado: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logger.info(user_sql)
    logger.info(database_sql)
    logger.info(host_sql)
    logger.info(password_sql)


# Chamado_status_historico
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.chamado_status_historico")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logger.info("A consulta não retornou nenhum dado.")
            else:
                for row in rows:
                    logger.info(f"Registro encontrado: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logger.info(user_sql)
    logger.info(database_sql)
    logger.info(host_sql)
    logger.info(password_sql)


# cliente_organizacao
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.cliente_organizacao")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logger.info("A consulta não retornou nenhum dado.")
            else:
                for row in rows:
                    logger.info(f"Registro encontrado: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logger.info(user_sql)
    logger.info(database_sql)
    logger.info(host_sql)
    logger.info(password_sql)


# analista
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.analista")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logger.info("A consulta não retornou nenhum dado.")
            else:
                for row in rows:
                    logger.info(f"Registro encontrado: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logger.info(user_sql)
    logger.info(database_sql)
    logger.info(host_sql)
    logger.info(password_sql)


# categoria
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.categoria")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logger.info("A consulta não retornou nenhum dado.")
            else:
                for row in rows:
                    logger.info(f"Registro encontrado: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logger.info(user_sql)
    logger.info(database_sql)
    logger.info(host_sql)
    logger.info(password_sql)


# csat_avaliacao e fila
# Primeira função - duda
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:

    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM itsm.csat_avaliacao")
            rows = cursor.fetchall()

            if not rows:
                logger.info("A tabela csat_avaliacao não retornou dados.")
            else:
                for row in rows:
                    logger.info(f"CSAT_AVALIACAO: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar a tabela csat_avaliacao: {e}")


# Segunda função - duda
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_fila(myTimer: func.TimerRequest) -> None:

    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM itsm.fila")
            rows = cursor.fetchall()

            if not rows:
                logger.info("A tabela fila não retornou dados.")
            else:
                for row in rows:
                    logger.info(f"FILA: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar a tabela fila: {e}")


# sla
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM itsm.sla")
            rows = cursor.fetchall()

            if not rows:
                logger.info("A tabela sla não retornou dados.")
            else:
                for row in rows:
                    logger.info(f"SLA: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar a tabela sla: {e}")


# solicitante
@app.timer_trigger(
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM itsm.solicitante")
            rows = cursor.fetchall()

            if not rows:
                logger.info("A tabela solicitante não retornou dados.")
            else:
                for row in rows:
                    logger.info(f"SOLICITANTE: {row}")

    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ou consultar a tabela solicitante: {e}")
