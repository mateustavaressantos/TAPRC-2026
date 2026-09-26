import logging
import azure.functions as func
import os
import urllib.request
import urllib.parse

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True) 
def timer_trigger_2(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

# Autenticação restrita via API Key (FUNCTION)
@app.route(route="http_trigger", auth_level=func.AuthLevel.FUNCTION)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    
    return func.HttpResponse(
        "Pass a name in the query string or in the request body for a personalized response.",
        status_code=200
    )

# Autenticação restrita via API Key (FUNCTION)
@app.route(route="http_trigger_v3", auth_level=func.AuthLevel.FUNCTION)
def http_trigger_v3(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
            name = req_body.get('name')
        except ValueError:
            pass

    if name:
        return func.HttpResponse(f"Valor recebido: {name}. - respondido pela http_trigger_v3.")
    
    return func.HttpResponse(
        "Pass a name in the query string or in the request body for a personalized response.",
        status_code=200
    )

@app.schedule(schedule="0 0 * * * *", arg_name="callTimer", run_on_startup=False, use_monitor=True)
def timer_trigger_chama_http(callTimer: func.TimerRequest) -> None:
    if callTimer.past_due:
        logging.info('O timer (chamador) esta atrasado!')

    # A URL deve vir 100% de variável de ambiente (App Settings na Azure)
    base_url = os.environ.get("ECHO_FUNCTION_URL")
    if not base_url:
        logging.warning("ECHO_FUNCTION_URL não configurada no ambiente.")
        return

    name = "ola-do-timer"
    # Se a rota exige FUNCTION key, inclua a chave na URL (?code=...) ou configure via headers
    url = f"{base_url}?{urllib.parse.urlencode({'name': name})}"

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as response:
            resposta = response.read().decode("utf-8")
            logging.info(f"Resposta da http_trigger_v3: {resposta}")
    except Exception as e:
        logging.error(f"Erro ao chamar a http_trigger_v3: {e}")

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    usuario = os.getenv("USER")
    banco_dados = os.getenv("DATABASE")
    senha = os.getenv("PASSWORD")
    servidor = os.getenv("HOST")

    logging.info(usuario)
    logging.error(banco_dados)
    logging.info(senha)
    print(servidor)