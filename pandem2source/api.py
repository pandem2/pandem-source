from flask import Flask, jsonify
from flasgger import Swagger
from flasgger import Swagger
from flask_restful import Api, Resource

app = Flask(__name__)
swagger = Swagger(app)

def run(conf, port, roles = None):
 app = Flask(__name__)
 api = Api(app)
 swagger = Swagger(app)
 if "driver" in roles:
   api.add_resource(WorkerConfig.create(conf), '/worker_config/<worker>') 
 app.run(debug = True, port = port)

class WorkerConfig(Resource):
  conf = {} 
  @classmethod
  def create(cls, conf):
    cls.conf = conf
    return cls

  def get(self, worker):
    """Endpoint for providing configurations setiings for workers
    ---
    parameters:
      - name: worker
        in: path
        type: string
        required: true
        default: all
    definitions:
      Settings:
        type: object
        additionamProperties: true
    responses:
      200:
        description: A map of settings for the requested worker
        schema:
          $ref: '#/definitions/Settings'
        examples:
          port: 8080
          log_endpoint: 'http://localhost:7777/logs'
    """
    if worker == 'all':
        result = self.conf.find() 
    else:
        result = self.conf.find(worker)

    return jsonify(result)

