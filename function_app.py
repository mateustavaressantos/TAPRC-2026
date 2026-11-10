import csv
import io
import logging
import os
from datetime import UTC, date, datetime, time

import azure.functions as func
import pyodbc
from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()
logger = logging.getLogger(__name__)

SCHEMA = "itsm"

# Pais antes dos filhos (respeita as FKs na hora de carregar no banco)
TABELAS_ORDENADAS = [
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

# Horários (UTC) de cada grupo de triggers
SCHEDULE_EXTRACT = "0 45 0 * * *"
SCHEDULE_LOAD = "0 48 0 * * *"
SCHEDULE_EXTRACT_LOAD = "0 51 0 * * *"
SCHEDULE_EXTRACT_ALL = "0 54 0 * * *"
SCHEDULE_LOAD_ALL = "0 57 0 * * *"
SCHEDULE_EXTRACT_LOAD_ALL = "0 0 1 * * *"


# Funções auxiliares
def _table_metadata(cursor, schema: str, table_name: str):
    # Colunas da tabela de destino
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        schema,
        table_name,
    )
    columns = {
        row[0].lower(): (row[0], row[1].lower(), row[2] == "YES")
        for row in cursor.fetchall()
    }
    if not columns:
        raise ValueError(f"Tabela {schema}.{table_name} não existe no banco de destino")

    # Colunas da chave primária
    cursor.execute(
        "SELECT kcu.COLUMN_NAME "
        "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
        "  ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
        " AND kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
        " AND kcu.TABLE_NAME = tc.TABLE_NAME "
        "WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
        "  AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? "
        "ORDER BY kcu.ORDINAL_POSITION",
        schema,
        table_name,
    )
    pk_cols = [row[0] for row in cursor.fetchall()]

    # Colunas IDENTITY
    cursor.execute(
        "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) AND is_identity = 1",
        f"{schema}.{table_name}",
    )
    identity_cols = [row[0] for row in cursor.fetchall()]

    return columns, pk_cols, identity_cols


def _convert(value: str, data_type: str, nullable: bool):
    # Converte o texto do CSV para o tipo da coluna no banco
    if value == "":
        return None if nullable else value
    if data_type in ("datetime", "datetime2", "smalldatetime"):
        return datetime.fromisoformat(value)
    if data_type == "date":
        return date.fromisoformat(value[:10])
    if data_type == "time":
        return time.fromisoformat(value)
    if data_type == "bit":
        return value.strip().lower() in ("1", "true")
    return value


# Função 1: extrair do banco de origem para o Data Lake (bronze)
def extract_table_to_datalake(schema: str, table_name: str) -> None:
    logger.info(f"Extraindo {schema}.{table_name}")

    # 1. Variáveis de ambiente (configuradas no Function App)
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = (os.getenv("PASSWORD") or "").replace("}", "}}")

    # 2. String de conexão
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={{{sql_pass}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 3. Leitura da tabela no banco de origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = f"select * from {schema}.{table_name}"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de {schema}.{table_name}")

        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
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
        logger.error(f"Erro ao extrair {schema}.{table_name}: {e!s}")
        raise


# Função 2: carregar do Data Lake (bronze) para o banco de destino
def load_datalake_to_sql(
    schema: str, table_name: str, run_date: date | None = None
) -> int:
    logger.info(f"Carregando {schema}.{table_name}")

    # 1. Variáveis de ambiente (configuradas no Function App)
    datalake_conn_str = os.getenv("DATALAKE_CONNECTION_STRING")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = (os.getenv("DEST_PASSWORD") or "").replace("}", "}}")

    if not all([datalake_conn_str, dest_server, dest_database, dest_user, dest_pass]):
        raise RuntimeError(
            "Variáveis de ambiente ausentes (DATALAKE_CONNECTION_STRING, DEST_HOST, DEST_DATABASE, DEST_USER, DEST_PASSWORD)"
        )

    # 2. String de conexão
    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={{{dest_pass}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn = None
    try:
        # 3. Leitura do CSV mais recente da pasta do dia no Data Lake
        # Pasta do dia (UTC, igual ao extract)
        run_date = run_date or datetime.now(UTC).date()
        folder_path = f"{schema}/{table_name}/{run_date:%Y/%m/%d}"

        datalake = DataLakeServiceClient.from_connection_string(datalake_conn_str)
        file_system_client = datalake.get_file_system_client("bronze")

        try:
            file_paths = [
                p.name
                for p in file_system_client.get_paths(path=folder_path, recursive=False)
                if not p.is_directory and p.name.lower().endswith(".csv")
            ]
        except ResourceNotFoundError:
            file_paths = []

        if not file_paths:
            logger.info(f"Nenhum arquivo CSV encontrado em bronze/{folder_path}")
            return 0

        # O nome do arquivo tem timestamp => max = mais novo
        latest_file_path = max(file_paths)
        logger.info(f"Iniciando leitura do arquivo: {latest_file_path}")

        file_client = file_system_client.get_file_client(latest_file_path)
        csv_string = file_client.download_file().readall().decode("utf-8-sig")

        csv_reader = csv.reader(io.StringIO(csv_string, newline=""))
        columns = next(csv_reader, None)
        rows = [row for row in csv_reader if row]

        if not columns or not rows:
            logger.info("O arquivo CSV está vazio (apenas cabeçalho).")
            return 0

        if any(len(row) != len(columns) for row in rows):
            raise ValueError(
                f"{latest_file_path}: há linhas com número de colunas diferente do cabeçalho"
            )

        # 4. Preparação dos dados conforme os tipos da tabela de destino
        target = f"[{schema}].[{table_name}]"
        dest_conn = pyodbc.connect(dest_conn_str)
        dest_cursor = dest_conn.cursor()

        meta, pk_cols, identity_cols = _table_metadata(dest_cursor, schema, table_name)

        missing = [c for c in columns if c.lower() not in meta]
        if missing:
            raise ValueError(
                f"Colunas do CSV que não existem em {schema}.{table_name}: {missing}"
            )

        col_defs = [meta[c.lower()] for c in columns]
        real_cols = [d[0] for d in col_defs]
        data = [
            tuple(_convert(v, d[1], d[2]) for v, d in zip(row, col_defs, strict=True))
            for row in rows
        ]

        quoted = ", ".join(f"[{c}]" for c in real_cols)
        placeholders = ", ".join(["?"] * len(real_cols))
        identity_lower = {c.lower() for c in identity_cols}
        pk_lower = {c.lower() for c in pk_cols}
        has_identity = any(c.lower() in identity_lower for c in real_cols)

        # 5. Gravação no banco de dados de destino (MERGE via tabela temporária)
        # Carrega o CSV numa tabela temporária com a estrutura do destino
        dest_cursor.execute(f"SELECT {quoted} INTO #stage FROM {target} WHERE 1 = 0")
        if has_identity:
            dest_cursor.execute("SET IDENTITY_INSERT #stage ON")
        dest_cursor.executemany(
            f"INSERT INTO #stage ({quoted}) VALUES ({placeholders})", data
        )
        if has_identity:
            dest_cursor.execute("SET IDENTITY_INSERT #stage OFF")

        if has_identity:
            dest_cursor.execute(f"SET IDENTITY_INSERT {target} ON")

        if pk_cols:
            if not pk_lower.issubset({c.lower() for c in real_cols}):
                raise ValueError(
                    f"CSV não contém todas as colunas da PK de {schema}.{table_name}: {pk_cols}"
                )
            on_clause = " AND ".join(f"t.[{k}] = s.[{k}]" for k in pk_cols)
            update_cols = [
                c
                for c in real_cols
                if c.lower() not in pk_lower and c.lower() not in identity_lower
            ]
            merge_sql = f"MERGE {target} AS t USING #stage AS s ON {on_clause} "
            if update_cols:
                set_clause = ", ".join(f"t.[{c}] = s.[{c}]" for c in update_cols)
                merge_sql += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            insert_vals = ", ".join(f"s.[{c}]" for c in real_cols)
            merge_sql += (
                f"WHEN NOT MATCHED BY TARGET THEN INSERT ({quoted}) "
                f"VALUES ({insert_vals});"
            )
            dest_cursor.execute(merge_sql)
        else:
            logger.warning(
                f"{schema}.{table_name} não tem PK: usando INSERT simples (pode duplicar)"
            )
            dest_cursor.execute(
                f"INSERT INTO {target} ({quoted}) SELECT {quoted} FROM #stage"
            )

        if has_identity:
            dest_cursor.execute(f"SET IDENTITY_INSERT {target} OFF")

        dest_conn.commit()

        logger.info(
            f"{len(rows)} linhas gravadas em {schema}.{table_name} no banco de destino"
        )
        return len(rows)

    except Exception as e:
        if dest_conn is not None:
            dest_conn.rollback()
        logger.error(f"Erro ao carregar {schema}.{table_name}: {e!s}")
        raise
    finally:
        if dest_conn is not None:
            dest_conn.close()


# Erros esperados ao processar uma tabela (permitem seguir para a próxima)
ERROS_TABELA = (
    pyodbc.Error,
    AzureError,
    OSError,
    ValueError,
    RuntimeError,
)


# Funções todas as tabelas
def extract_all_tables() -> dict:
    logger.info("Iniciando a extração de todas as tabelas...")
    falhas: dict = {}

    for tabela in TABELAS_ORDENADAS:
        try:
            extract_table_to_datalake(SCHEMA, tabela)
        except ERROS_TABELA as e:
            falhas[tabela] = str(e)
            logger.error(f"Falha ao extrair {SCHEMA}.{tabela}: {e!s}")

    logger.info("Extração de todas as tabelas concluída!")
    return falhas


def load_all_tables() -> dict:
    logger.info("Iniciando o carregamento de todas as tabelas...")
    falhas: dict = {}

    for tabela in TABELAS_ORDENADAS:
        try:
            load_datalake_to_sql(SCHEMA, tabela)
        except ERROS_TABELA as e:
            falhas[tabela] = str(e)
            logger.error(f"Falha ao carregar {SCHEMA}.{tabela}: {e!s}")

    logger.info("Carregamento de todas as tabelas concluído!")
    return falhas


def _raise_if_failed(falhas: dict) -> None:
    # Faz a execução aparecer como falha no monitoramento do Azure
    if falhas:
        raise RuntimeError(f"Falha em {len(falhas)} tabela(s): {falhas}")


# TRIGGERS 1: extrair (banco de origem -> Data Lake), uma por tabela
@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "categoria")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "cliente_organizacao")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_fila(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "fila")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "sla")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "analista")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "solicitante")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "chamado")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "chamado_sla")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "chamado_status_historico")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "csat_avaliacao")


# TRIGGERS 2: carregar (Data Lake -> banco de destino), uma por tabela
@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_categoria(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "categoria")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "cliente_organizacao")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_fila(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "fila")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_sla(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "sla")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_analista(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "analista")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_solicitante(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "solicitante")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_chamado(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "chamado")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_chamado_sla(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "chamado_sla")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "chamado_status_historico")


@app.timer_trigger(
    schedule=SCHEDULE_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    load_datalake_to_sql(SCHEMA, "csat_avaliacao")


# TRIGGERS 3: extrair e carregar, uma por tabela
@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_categoria(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "categoria")
    load_datalake_to_sql(SCHEMA, "categoria")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "cliente_organizacao")
    load_datalake_to_sql(SCHEMA, "cliente_organizacao")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_fila(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "fila")
    load_datalake_to_sql(SCHEMA, "fila")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "sla")
    load_datalake_to_sql(SCHEMA, "sla")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_analista(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "analista")
    load_datalake_to_sql(SCHEMA, "analista")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_solicitante(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "solicitante")
    load_datalake_to_sql(SCHEMA, "solicitante")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_chamado(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "chamado")
    load_datalake_to_sql(SCHEMA, "chamado")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_chamado_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "chamado_sla")
    load_datalake_to_sql(SCHEMA, "chamado_sla")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "chamado_status_historico")
    load_datalake_to_sql(SCHEMA, "chamado_status_historico")


@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake(SCHEMA, "csat_avaliacao")
    load_datalake_to_sql(SCHEMA, "csat_avaliacao")


# TRIGGERS 4: todas as tabelas de uma vez
# Extrai todas as tabelas do banco de origem para o Data Lake
@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_ALL,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_all(myTimer: func.TimerRequest) -> None:
    _raise_if_failed(extract_all_tables())


# Carrega todas as tabelas do Data Lake para o banco de destino
@app.timer_trigger(
    schedule=SCHEDULE_LOAD_ALL,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_all(myTimer: func.TimerRequest) -> None:
    _raise_if_failed(load_all_tables())


# Extrai e carrega todas as tabelas (extrai tudo primeiro, depois carrega)
@app.timer_trigger(
    schedule=SCHEDULE_EXTRACT_LOAD_ALL,
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_load_all(myTimer: func.TimerRequest) -> None:
    falhas_extract = extract_all_tables()
    falhas_load = load_all_tables()

    _raise_if_failed(
        {
            **{f"extract:{t}": e for t, e in falhas_extract.items()},
            **{f"load:{t}": e for t, e in falhas_load.items()},
        }
    )
