# https://apispec.readthedocs.io/en/latest/

import yaml
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
import json
from flask import Flask
from apis.executor import ExecuteScript
from apis.venv import CreateVenv

marshmallow_plugin = MarshmallowPlugin()

OPENAPI_SPEC = """
servers:
- url: http://localhost:5000/simple-task-executor
  description: The development API server
"""

settings = yaml.safe_load(OPENAPI_SPEC)

spec = APISpec(
    title="Simple Task Executor",
    version="1.0.0",
    openapi_version="3.0.3",
    plugins=[FlaskPlugin(), marshmallow_plugin],
    **settings
)

app = Flask(__name__)

mv_ex = ExecuteScript.as_view("/executor/execute-script/")
app.add_url_rule("/executor/execute-script/", view_func=mv_ex)

mv_cv = CreateVenv.as_view("/venv/create-venv/")
app.add_url_rule("/venv/create-venv/", view_func=mv_cv)


with app.test_request_context():
    spec.path(view=mv_ex)
    spec.path(view=mv_cv)

# scrivo lo swagger in json

with open('static/swagger.json', 'w', encoding='utf-8') as f:
    json.dump(spec.to_dict(), f, ensure_ascii=False, indent=4)
