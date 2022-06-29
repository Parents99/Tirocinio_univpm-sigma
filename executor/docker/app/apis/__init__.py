from flask_restx import Api

from .executor import api as executor
from .venv import api as venv

def create_app():
    api = Api(
        title='Rest services for task executor',
        version='1.0',
        description='A description',
        prefix="/simple-task-executor",
    )

    api.add_namespace(executor, path='/executor')
    api.add_namespace(venv, path='/venv')

    return api
