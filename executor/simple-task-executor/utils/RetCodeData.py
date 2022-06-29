import enum
import json


class RetCode(enum.Enum):
    UNKNOWN = 1
    SUCCESS = 2
    WARNING = 3
    ERROR = 4


class RetCodeData:

    def __init__(self, ret_code=RetCode.UNKNOWN.name, ret_code_message=None):
        self.ret_code = ret_code
        self.ret_code_message = ret_code_message

    def json_format(self):
        return json.loads(json.dumps(self.__dict__))
