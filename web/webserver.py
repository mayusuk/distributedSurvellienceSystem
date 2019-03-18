#!flask/bin/python
from flask import Flask, jsonify
from flask import render_template
import urllib.request
import cgi 
import boto3
import subprocess
import threading
import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
print(sys.path)

from util.util import Util

app = Flask(__name__, instance_relative_config=True)
config = Util().get_config()

@app.route('/', methods=['GET'])
def get_objects():
    url = config,get('dev', 'VIDEO_URL')

    remotefile = urllib.request.urlopen(url)
    blah = remotefile.info()['Content-Disposition']
    _, params = cgi.parse_header(blah)

    
    filename = params["filename"]
    d = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    filepath = os.path.join(d, "data", filename)

    result = urllib.request.urlretrieve(url, filepath)
    filepath = result[0]

    bucket_name = config,get('dev','BUCKET_NAME')
    thread = threading.Thread(target=upload_to_s3, args=(bucket_name, filename, filepath))
    # if obj is not None:
    #     thread.run()
    #     return render_template('object.html', obj=[1,2,2,3])
    # else:
    #     return render_template('error.html')
    

def upload_to_s3(bukcet_name, key, content):
    s3 = boto3.resource("s3")
    s3.Object(bukcet_name, key).put(Body=content)


if __name__ == '__main__':
    app.run(debug=True)
