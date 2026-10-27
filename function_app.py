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
    schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.chamado_sla"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.chamado_sla")

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
        file_path = (
            f"itsm/chamado_sla/{now:%Y/%m/%d}/chamado_sla_{now:%Y%m%d_%H%M%S}.csv"
        )

        # itsm/chamado_sla/year=2024/month=06/day=05/chamado_sla_20240605_123456.csv
        # file_path = f"itsm/chamado_sla/year={now:%Y}/month={now:%m}/day={now:%d}/chamado_sla_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.chamado_sla: {e!s}")
        raise


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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.chamado_status_historico"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.chamado_status_historico")

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
        file_path = f"itsm/chamado_status_historico/{now:%Y/%m/%d}/chamado_status_historico_{now:%Y%m%d_%H%M%S}.csv"

        # itsm/chamado_status_historico/year=2024/month=06/day=05/chamado_status_historico_20240605_123456.csv
        # file_path = f"itsm/chamado_status_historico/year={now:%Y}/month={now:%m}/day={now:%d}/chamado_status_historico_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.chamado_status_historico: {e!s}")
        raise


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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.cliente_organizacao"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.cliente_organizacao")

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
        file_path = f"itsm/cliente_organizacao/{now:%Y/%m/%d}/cliente_organizacao_{now:%Y%m%d_%H%M%S}.csv"

        # itsm/cliente_organizacao/year=2024/month=06/day=05/cliente_organizacao_20240605_123456.csv
        # file_path = f"itsm/cliente_organizacao/year={now:%Y}/month={now:%m}/day={now:%d}/cliente_organizacao_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.cliente_organizacao: {e!s}")
        raise


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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.analista"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.analista")

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
        file_path = f"itsm/analista/{now:%Y/%m/%d}/analista_{now:%Y%m%d_%H%M%S}.csv"

        # itsm/analista/year=2024/month=06/day=05/analista_20240605_123456.csv
        # file_path = f"itsm/analista/year={now:%Y}/month={now:%m}/day={now:%d}/analista_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.analista: {e!s}")
        raise


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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.categoria"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.categoria")

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
        file_path = f"itsm/categoria/{now:%Y/%m/%d}/categoria_{now:%Y%m%d_%H%M%S}.csv"

        # itsm/categoria/year=2024/month=06/day=05/categoria_20240605_123456.csv
        # file_path = f"itsm/categoria/year={now:%Y}/month={now:%m}/day={now:%d}/categoria_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.categoria: {e!s}")
        raise


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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.csat_avaliacao"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.csat_avaliacao")

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
        file_path = (
            f"itsm/csat_avaliacao/{now:%Y/%m/%d}/csat_avaliacao_{now:%Y%m%d_%H%M%S}.csv"
        )

        # itsm/csat_avaliacao/year=2024/month=06/day=05/csat_avaliacao_20240605_123456.csv
        # file_path = f"itsm/csat_avaliacao/year={now:%Y}/month={now:%m}/day={now:%d}/csat_avaliacao_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.csat_avaliacao: {e!s}")
        raise


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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.fila"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.fila")

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
        file_path = f"itsm/fila/{now:%Y/%m/%d}/fila_{now:%Y%m%d_%H%M%S}.csv"

        # itsm/fila/year=2024/month=06/day=05/fila_20240605_123456.csv
        # file_path = f"itsm/fila/year={now:%Y}/month={now:%m}/day={now:%d}/fila_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.fila: {e!s}")
        raise


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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.sla"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.sla")

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
        file_path = f"itsm/sla/{now:%Y/%m/%d}/sla_{now:%Y%m%d_%H%M%S}.csv"

        # itsm/sla/year=2024/month=06/day=05/sla_20240605_123456.csv
        # file_path = f"itsm/sla/year={now:%Y}/month={now:%m}/day={now:%d}/sla_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.sla: {e!s}")
        raise


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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = "SELECT * FROM itsm.solicitante"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de itsm.solicitante")

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
        file_path = (
            f"itsm/solicitante/{now:%Y/%m/%d}/solicitante_{now:%Y%m%d_%H%M%S}.csv"
        )

        # itsm/solicitante/year=2024/month=06/day=05/solicitante_20240605_123456.csv
        # file_path = f"itsm/solicitante/year={now:%Y}/month={now:%m}/day={now:%d}/solicitante_{now:%Y%m%d_%H%M%S}.csv"

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
        logger.error(f"Erro ao ler itsm.solicitante: {e!s}")
        raise
