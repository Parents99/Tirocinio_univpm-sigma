import ast
import json
import os
import sys
import importlib.util
from connection import conn_options


def define_conn_pool(coordinates, type):
    json_coordinates = json.loads(coordinates.replace("'", '"'))
    connection_pool = {}  # creo un pool vuoto come un dizionario
    for json_c in json_coordinates:  # per ogni connessione
        conn_type = json_c[type]
        connection = conn_options[conn_type]()  # in base al tipo di connessione viene invocato il corrispondente metodo per la sua creazione
        connection_pool[conn_type] = {}  # definisco un dizionario interno per ogni tipo di connessione
        connection_pool[conn_type]["infos"] = json_c  # aggiungo al dizionario interno i parametri per la connessione
        connection_pool[conn_type]["connObject"] = connection  # aggiungo al dizionario interno l'oggetto della connessione
    return connection_pool


def sigma_main(partner_script, method, params, input_coordinates, output_coordinates, session_id):
    # definisco le connessioni di input e di output
    input_connection_pool = define_conn_pool(input_coordinates, "sourceType")
    output_connection_pool = define_conn_pool(output_coordinates, "destType")
    partner_args = ast.literal_eval(params)

    # spec = importlib.util.spec_from_file_location("atm_logs.simulated-streaming-test", partner_script)
    # os.chdir("C:\GIT\simple-rd\simple-agile\ExecuteScriptHTTPService\partner-volume\partner_scripts")
    # partner_module = importlib.util.module_from_spec(spec)
    # spec.loader.exec_module(partner_module)
    # partner_method = getattr(partner_module, method)
    # partner_method(input_connection_pool, output_connection_pool)

    # importo dinamicamente le librerie presenti nel partner-volume
    partner_partition = os.environ['PARTNER_VOLUME_PATH']
    sys.path.append(partner_partition + 'partner_scripts')
    partner_module = importlib.import_module(partner_script)
    partner_method = getattr(partner_module, method)
    return partner_method(input_connection_pool, output_connection_pool, *partner_args,session_id=session_id)  # chiamo il "method" che deve essere eseguito passandogli le connessioni di input e di output


# punto di ingresso del sottoprocesso che viene lanciato dal Web Server
if __name__ == '__main__':

    # leggo gli argomenti di ingresso
    partner_script = sys.argv[1]
    method = sys.argv[2]
    params = sys.argv[3]
    input_coordinates = sys.argv[4]
    output_coordinates = sys.argv[5]
    session_id=sys.argv[6]
    output_result = sigma_main(partner_script, method, params, input_coordinates, output_coordinates, session_id)
