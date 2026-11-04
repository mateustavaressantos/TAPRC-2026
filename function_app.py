import csv
import io
import logging
import os
from datetime import UTC, datetime

import azure.functions as func
import pyodbc
from azure.core.exceptions import AzureError
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()
logger = logging.getLogger(__name__)


def load_datalake_to_sql(schema: str, table_name: str) -> None:
    """Função genérica para ler o CSV mais recente do ADLS (Bronze) e inserir no SQL Server."""

    # 1. Configurar credenciais
    datalake_conn_str = os.getenv("DATALAKE_CONNECTION_STRING")
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    sql_conn_str = (
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
        # 2. Localizar o arquivo do dia atual no Data Lake
        now = datetime.now(UTC)
        # Caminho baseado na estrutura da imagem: bronze/itsm/analista/YYYY/MM/DD/
        folder_path = f"{schema}/{table_name}/{now:%Y/%m/%d}/"

        datalake = DataLakeServiceClient.from_connection_string(datalake_conn_str)
        file_system_client = datalake.get_file_system_client("bronze")

        # Busca todos os arquivos dentro da pasta do dia atual
        paths = file_system_client.get_paths(path=folder_path)
        file_paths = [p.name for p in paths if not p.is_directory]

        if not file_paths:
            logger.info(f"Nenhum arquivo CSV encontrado em bronze/{folder_path}")
            return

        # Pega o arquivo mais recente pela ordem alfabética do timestamp no nome
        latest_file_path = max(file_paths)
        logger.info(f"Iniciando leitura do arquivo: {latest_file_path}")

        # 3. Baixar e ler o CSV para a memória
        file_client = file_system_client.get_file_client(latest_file_path)
        csv_bytes = file_client.download_data().readall()
        csv_string = csv_bytes.decode("utf-8")

        csv_reader = csv.reader(io.StringIO(csv_string))
        columns = next(csv_reader)  # Pega a primeira linha (cabeçalhos)
        rows = [row for row in csv_reader]  # Pega o restante dos dados

        if not rows:
            logger.info("O arquivo CSV está vazio (apenas cabeçalho).")
            return

        # 4. Montar a query dinâmica de INSERT
        # Transforma a lista de colunas em string: "col1, col2, col3"
        col_names = ", ".join(columns)
        # Cria as interrogações para a parametrização: "?, ?, ?"
        placeholders = ", ".join(["?"] * len(columns))

        query = (
            f"INSERT INTO {schema}.{table_name} ({col_names}) VALUES ({placeholders})"
        )

        # 5. Conectar ao banco e executar a inserção em massa (Bulk Insert)
        with pyodbc.connect(sql_conn_str) as conn:
            cursor = conn.cursor()

            # Habilita a inserção manual de IDs (ignora erro se a tabela não tiver IDENTITY)
            try:
                cursor.execute(f"SET IDENTITY_INSERT {schema}.{table_name} ON")
            except pyodbc.Error:
                pass

            # executemany é otimizado para inserir milhares de linhas rapidamente
            cursor.executemany(query, rows)

            # Desabilita a inserção manual de IDs
            try:
                cursor.execute(f"SET IDENTITY_INSERT {schema}.{table_name} OFF")
            except pyodbc.Error:
                pass

            conn.commit()

        logger.info(
            f"{len(rows)} linhas inseridas com sucesso na tabela {schema}.{table_name}!"
        )

    except Exception as e:
        logger.error(
            f"Erro ao carregar dados do Data Lake para a tabela {schema}.{table_name}: {e!s}"
        )
        raise


def extract_table_to_datalake(schema: str, table_name: str) -> None:
    """Função genérica para extrair dados do SQL Server e salvar no ADLS (Bronze)."""
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
            query = f"SELECT * FROM {schema}.{table_name}"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de {schema}.{table_name}")

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        now = datetime.now(UTC)
        file_path = (
            f"{schema}/{table_name}/{now:%Y/%m/%d}/{table_name}_{now:%Y%m%d_%H%M%S}.csv"
        )

        datalake = DataLakeServiceClient.from_connection_string(
            os.getenv("DATALAKE_CONNECTION_STRING")
        )
        file_client = datalake.get_file_system_client("bronze").get_file_client(
            file_path
        )

        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)
        logger.info(f"Arquivo gravado em bronze/{file_path}")

    except Exception as e:
        logger.error(f"Erro ao processar a tabela {schema}.{table_name}: {e!s}")
        raise


SCHEDULE_TIME = "0 55 23 * * *"


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado_sla")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado_status_historico")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "cliente_organizacao")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "analista")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "categoria")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csat(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "csat")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "csat_avaliacao")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_fila(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "fila")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "sla")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "solicitante")


@app.timer_trigger(
    schedule="0 45 0 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def load_all_bronze_to_sql(myTimer: func.TimerRequest) -> None:
    tabelas_ordenadas = [
        "categoria",
        "cliente_organizacao",
        "fila",
        "sla",
        "analista",
        "solicitante",
        "chamado",
        "chamado_sla",
        "chamado_status_historico",
        "csat_avaliacao",
    ]

    logger.info(
        "A iniciar o carregamento em massa (Bulk Insert) do Data Lake para o SQL..."
    )

    for tabela in tabelas_ordenadas:
        try:
            logger.info(f"A processar tabela: itsm.{tabela}")
            load_datalake_to_sql("itsm", tabela)

        except (pyodbc.Error, AzureError, ValueError) as e:
            logger.error(f"Falha ao carregar a tabela itsm.{tabela}: {e!s}")

    logger.info("Carregamento em massa concluído!")
