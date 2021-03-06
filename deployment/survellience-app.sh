#! /bin/bash

pip3 install --user flask
pip3 install --user boto3

export PATH=$PATH:/root/.local/bin
export FLASK_APP=workerMain
export AWS_DEFAULT_REGION=us-west-1
export AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY

cd /home/ubuntu/distributedSurvellienceSystem

flask run --host=0.0.0.0 --port=5000 
