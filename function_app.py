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
        file_path = f"{schema}/{table_name}/{now:%Y/%m/%d}/{table_name}_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(
            os.getenv("DATALAKE_CONNECTION_STRING")
        )
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)
        logger.info(f"Arquivo gravado em bronze/{file_path}")

    except Exception as e:
        logger.error(f"Erro ao processar a tabela {schema}.{table_name}: {e!s}")
        raise

SCHEDULE_TIME = "0 25 20 * * *"

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado_sla")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado_status_historico")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "cliente_organizacao")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_analista(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "analista")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "categoria")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "csat")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "csat_avaliacao")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_fila(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "fila")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "sla")

@app.timer_trigger(schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "solicitante")