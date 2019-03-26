# distributedSurvellienceSystem

install xvfb
sudo apt install xvfb

sudo apt install python3-pip

install flask
pip3 install --user flask

pip3 install --user boto3

copy credentials and config for aws account to ~/.aws

create 2 queues one for requests and other for response


import boto3
client = boto3.client('sqs')
response = client.create_queue(
    QueueName='test2.fifo',
    Attributes={
        'FifoQueue': 'true',
	'ContentBasedDeduplication' : 'true'
    }
)

create 2 s3 buckets one fore storing videos and other to store response


copy new weight file 

FLASK_APP=workerMain
flask run --host=0.0.0.0 --port=5000


StandardOutput=file:/var/log1.log
StandardError=file:/var/log2.log