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