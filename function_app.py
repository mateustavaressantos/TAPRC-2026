import logging
import azure.functions as func
import os
import pyodbc

app = func.FunctionApp()

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def extract_chamado(myTimer: func.TimerRequest) -> None:
    #importar as variaveis de ambeinte 
    host_sql = os.getenv("HOST")
    database_sql = os.getenv("DATABASE")
    user_sql = os.getenv("USER")
    password_sql = os.getenv("PASSWORD")

    #como criar uma connection string usando pyodbc
    conn = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={password_sql};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    logging.info("Iniciando conexão com o Azure SQL Database...")

    try:
        # Criar a conexão com o banco usando gerenciador de contexto
        with pyodbc.connect(conn) as conn:
            with conn.cursor() as cursor:
                
                # Fazer um select * na tabela
                cursor.execute(f"SELECT * FROM itsm.chamado;")
                linhas = cursor.fetchall()
                
                logging.info(f"Total de registros encontrados: {len(linhas)}")
                
                # Imprimir os dados da tabela usando logging.info
                for linha in linhas:
                    # Converte a linha (tuple/row) para string para o log
                    logging.info(f"Registro: {list(linha)}")
                    
    except Exception as e:
        logging.error(f"Erro ao conectar ou executar query no banco: {str(e)}")