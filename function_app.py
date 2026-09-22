import logging
import azure.functions as func

app = func.FunctionApp()

def teste ():
    logging.info('Teste')

@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger_mateus(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function executed.')

    teste()

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger_2(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )