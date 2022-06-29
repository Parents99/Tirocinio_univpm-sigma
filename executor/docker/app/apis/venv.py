import os
import subprocess

from dynaconf import settings
from flask import request
from flask_restx import Resource, Namespace
from logger import log
from utils.RetCodeData import RetCodeData, RetCode
from utils.ValidateParam import CreateVenvSchema

api = Namespace('venv', description='Service that handles venvs')


@api.route('/create-venv')
class CreateVenv(Resource):

    def post(self):

        """CreateVenv
        ---
        description: api per la creazione di un venv
        requestBody:
            content:
              multipart/form-data:
                schema: CreateVenvSchema
        responses:
          201:
            description: OK
            content:
              application/json:
                schema: RetCodeDataSchema
        """
        log.debug("POST /create-venv/")
        ret = RetCodeData()
        validation_schema = CreateVenvSchema()  # definisco uno schema per la validazione del requestBody
        errors = validation_schema.validate(request.form)  # valido il requestBody
        if errors:  # Se l'input non è validato ritorno un messaggio di errore
            ret.ret_code = RetCode.ERROR.name
            ret.ret_code_message = errors
            log.error(ret.ret_code_message)
            return ret.json_format(), 422
        requirements_file = request.files.get("requirements") or None
        if requirements_file is not None:
            requirements_file.save("./scripts/" + requirements_file.filename)
            requirements_file = requirements_file.filename
        else:
            open("./scripts/requirements.txt", "w").close()
            requirements_file = "requirements.txt"
        python_version = request.form.get("pythonVersion") or settings.VENV.PYTHON_VERSION
        venv_name = request.form["venvName"]
        create_venv_path = ["./scripts/create-venv.sh", python_version, venv_name, requirements_file]
        subprocess.Popen(create_venv_path)
        ret.ret_code = RetCode.SUCCESS.name
        ret.ret_code_message = "Creating virtual environment..."
        log.info(ret.ret_code_message)
        return ret.json_format(), 201
