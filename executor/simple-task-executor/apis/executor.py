import os
import subprocess
from flask import request
from flask_restx import Resource, Namespace
from logger import log
from utils.RetCodeData import RetCodeData, RetCode
from utils.ValidateParam import ExecuteScriptSchema

api = Namespace('executor', description='Service for execut task')


@api.route('/execute-script')
class ExecuteScript(Resource):

    def post(self):

        """ExecuteScript
        ---
        description: api per l'esecuzione di uno script python
        requestBody:
            content:
              application/json:
                schema: ExecuteScriptSchema
        responses:
          200:
            description: OK
            content:
              application/json:
                schema: RetCodeDataSchema
        """
        log.debug("POST /execute-script/")
        ret = RetCodeData()
        content = request.json
        validation_schema = ExecuteScriptSchema()  # definisco uno schema per la validazione del requestBody
        errors = validation_schema.validate(content)  # valido il requestBody
        if errors:  # Se l'input non è validato ritorno un messaggio di errore
            ret.ret_code = RetCode.ERROR.name
            ret.ret_code_message = errors
            log.error(ret.ret_code_message)
            return ret.json_format(), 422
        # leggo i parametri di ingresso del requestBody
        session_id = content["sessionId"]
        script_path = content["scriptPath"]
        virtual_env = content["virtualEnv"]
        method = content["method"]
        params = content["params"]
        input_coordinates = content["inputCoordinates"]
        output_coordinates = content["outputCoordinates"]

        partner_partition = os.environ['PARTNER_VOLUME_PATH']  # viene settata automaticamente sul docker-compose
        python_path = partner_partition + "venvs/" + virtual_env + "/bin/python"  # python_path per un virtual environment generato su Linux
        #python_path = partner_partition + "venvs\\" + virtual_env + "\\Scripts\\python.exe"  # python_path per un virtual environment generato su Windows

        cmd = [python_path, "./generic_volume/sigma_wrapper.py", script_path, method, str(params), str(input_coordinates),
               str(output_coordinates),str(session_id)]  # costruisco il comando per lanciare il sottoprocesso
        log.info("Running command: '" + " ".join(cmd) + "'")
        subprocess.Popen(cmd)  # eseguo il sottoprocesso
        # così aspetto la fine dell'esecuzione
        # p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # for line in iter(p.stdout.readline, b''):  # b'' here for python3
        #     sys.stdout.write(line.decode(sys.stdout.encoding))
        #
        # error = p.stderr.read().decode()
        # if error:
        #     raise Exception(error)
        ret.ret_code = RetCode.SUCCESS.name
        ret.ret_code_message = "Execution with sessionId " + session_id + " started..."
        log.info(ret.ret_code_message)
        return ret.json_format(), 200



# @app.route('/api/listVenvs', methods=['GET'])
# def list_venvs():
#     root = "\\GIT\\simple-rd\\simple-agile\\venv\\"
#     dirlist = [item for item in os.listdir(root) if os.path.isdir(os.path.join(root, item))]
#     print(dirlist)
#     return str(dirlist)
