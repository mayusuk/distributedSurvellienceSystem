import threading
import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
print(sys.path)
from worker.sqsWorker import SQSWorker
from flask import Flask, jsonify, make_response
from util.util import Util
from util.logger import Logger
logger = Logger().get_logger(__name__)


def create_app():
    app = Flask(__name__)

    with app.app_context():
        config = Util().get_config()
        request_sqs = config.get('dev','REQUESTS_SQS')
        response_sqs = config.get('dev','RESPONSE_SQS')
        sqsworker = SQSWorker(60,request_sqs, response_sqs, 360, 1)    
        thread = threading.Thread(target=sqsworker.listener)
        sqsworker.recurrentDeleteMessageJob.start()
        thread.start()

    @app.route('/health', methods=['GET'])
    def get_health():
        result = {'Response': 'Done'}
        return make_response(jsonify(result), 200)
    
    @app.route('/averageResponseTime', methods=['GET'])
    def get_averageResponseTime():
        result = {'Response': 'Done'}
        return make_response(jsonify(result), 200)

    @app.route('/stop', methods=['GET'])
    def stop_listner():
        sqsworker.stopListner = True
        result = {'Response': 'Done'}
        return make_response(jsonify(result), 200)

    return app