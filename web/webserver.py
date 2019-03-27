#!flask/bin/python
from flask import Flask, jsonify, render_template, request
import urllib.request
import cgi 
import boto3
import subprocess
import threading
import os, sys
import time
import json
from collections import defaultdict


sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
print(sys.path)

from util.util import Util
from worker.sqsWorker import SQSWorker
lock = threading.Lock()

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    with app.app_context():
        config = Util().get_config()
        sqsworker = SQSWorker(60,"request.fifo", "response.fifo", 240, 1)
        _set = set()
        pollerThread = threading.Thread(target=sqsworker.response_poller)
        pollerThread.start()
        
    @app.route("/")
    def get_default():
        url = config.get('dev', 'VIDEO_URL')
        
        remotefile = urllib.request.urlopen(url)
        blah = remotefile.info()['Content-Disposition']
        _, params = cgi.parse_header(blah)
        filename = params["filename"]
        uploadFlag = True

        lock.acquire()
        if filename in _set:
            uploadFlag = False
        lock.release()

        d = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        filepath = os.path.join(d, "data", filename)
        bucket_name = config.get('dev','BUCKET_NAME')

        lock.acquire()
        if uploadFlag:
            result = urllib.request.urlretrieve(url, filepath)
            filepath = result[0]
            upload_to_s3(bucket_name, filepath, filename)
            _set.add(filename)
        lock.release()
        
        
        #thread = threading.Thread(target=upload_to_s3, args=(bucket_name, filename, filepath))
        #Upload the video to s3
        print(filepath, filename)
        message_attribute = {
                                'Filename': {
                                    'DataType': 'String',
                                    'StringValue': filename
                                }
                            }
        message_body = "Filename" + filename
        print(filename)
        groupid = "distributedSurvellienceSystem"
        messageId = sqsworker.send_message_sqs(filename, message_attribute, message_body, groupid)
        response = ResponseObject()
        #response.status = "Success"
        response.messageId = messageId
        response.videoId = filename

        thread = threading.Thread(target=wait_func, args=(messageId,))
        thread.start()
        
        thread.join()
        resp = sqsworker.responseDict[messageId]
        response.response = resp.strip()
        response.status = "Success"
        #_set.remove(filename)
        return "[" + response.videoId+","+response.response+"]"
        
    @app.route("/getMessageId")
    def getMessageId():
        url = config.get('dev', 'VIDEO_URL')

        remotefile = urllib.request.urlopen(url)
        blah = remotefile.info()['Content-Disposition']
        _, params = cgi.parse_header(blah)

        
        filename = params["filename"]
        d = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        filepath = os.path.join(d, "data", filename)

        result = urllib.request.urlretrieve(url, filepath)
        filepath = result[0]

        bucket_name = config.get('dev','BUCKET_NAME')
        
        #thread = threading.Thread(target=upload_to_s3, args=(bucket_name, filename, filepath))
        #Upload the video to s3
        print(filepath, filename)
        upload_to_s3(bucket_name, filepath, filename)
        message_attribute = {
                                'Filename': {
                                    'DataType': 'String',
                                    'StringValue': filename
                                }
                            }
        message_body = "Filename" + filename
        print(filename)
        groupid = "distributedSurvellienceSystem"
        messageId = sqsworker.send_message_sqs(filename, message_attribute, message_body, groupid)
        response = ResponseObject()
        response.status = "Success"
        response.messageId = messageId
        response.videoId = filename
        return json.dumps(response.__dict__)


    @app.route("/getResponse")
    def getResponse():
        response = ResponseObject()
        print(sqsworker.responseDict)
        messageId = request.args.get('messageId')
        print(messageId)

        if messageId in sqsworker.responseDict:
            resp = sqsworker.responseDict[messageId]
            response.response = resp
            response.status = "Success"
            response.messageId = messageId
            print('Found')
            
            
        print("SQS ",sqsworker.responseDict)
        return json.dumps(response.__dict__)


    def upload_to_s3(bukcet_name, filepath, filename):
            
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bukcet_name)
            bucket.upload_file(filepath, filename)
            #s3 = boto3.client("s3")
            #s3.upload_file(key,bukcet_name,content)

    def wait_func(messageId):
          while True:
            if messageId in sqsworker.responseDict:
                break
            time.sleep(2)

    class ResponseObject:
        def __init__(self):
            self.status = "Failure"
            self.response = None
            self.messageId= None
            self.videoId = None

    return app
application = create_app()