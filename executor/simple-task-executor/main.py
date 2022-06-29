from logger import log
from utils import apispec
from flask_swagger_ui import get_swaggerui_blueprint
from werkzeug.middleware.proxy_fix import ProxyFix
from flask import Flask
from apis import create_app
from dynaconf import settings

app = Flask(__name__)
create_app().init_app(app)


'''swagger specific'''
SWAGGER_URL = settings.GENERAL.URL_ROOT + '/api/v1/docs'
API_URL = '/static/swagger.json'
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)
app.wsgi_app = ProxyFix(app.wsgi_app)

log.info("\n####################################################################################################\n"
"#    _____                 _              _____                                _                   #\n"  
"#   |_   _|   __ _   ___  | | __         | ____| __  __   ___    ___   _   _  | |_    ___    _ __  #\n"
"#     | |    / _` | / __| | |/ /  _____  |  _|   \ \/ /  / _ \  / __| | | | | | __|  / _ \  | '__| #\n"
"#     | |   | (_| | \__ \ |   <  |_____| | |___   >  <  |  __/ | (__  | |_| | | |_  | (_) | | |    #\n"
"#     |_|    \__,_| |___/ |_|\_\         |_____| /_/\_\  \___|  \___|  \__,_|  \__|  \___/  |_|    #\n"
"####################################################################################################")

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5000, use_reloader=False)
