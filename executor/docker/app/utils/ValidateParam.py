# https://marshmallow.readthedocs.io/en/stable/

from marshmallow import Schema, fields
from marshmallow_enum import EnumField

from utils.RetCodeData import RetCode


class ExecuteScriptSchema(Schema):
    # the 'required' argument ensures the field exists
    sessionId = fields.Str(required=True)
    scriptPath = fields.Str(required=True)
    virtualEnv = fields.Str(required=False)
    method = fields.Str(required=True)
    params = fields.List(fields.Raw(), required=False)
    inputCoordinates = fields.List(fields.Dict(), required=False)
    outputCoordinates = fields.List(fields.Dict(), required=False)
    outputResult = fields.Str(required=False)


class CreateVenvSchema(Schema):
    # the 'required' argument ensures the field exists
    requirements = fields.Raw(type='file', required=False)
    pythonVersion = fields.Str(required=False)
    venvName = fields.Str(required=True)


class RetCodeDataSchema(Schema):
    ret_code = EnumField(RetCode)
    ret_code_message = fields.Str()