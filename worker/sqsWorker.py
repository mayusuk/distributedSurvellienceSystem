#!flask/bin/python

import boto3
import botocore
import queue
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import sys, os
import subprocess
import urllib.request
import cgi 
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
print(sys.path)
from os.path import dirname, abspath
from timer.repeatedTimer import RepeatedTimer
from util.util import Util
import signal
from util.logger import Logger

logger = Logger().get_logger(__name__)

class SQSWorker:

    def __init__(self, listenerTimeOut, requestQueueName, responseQueueName, visibilityTimeOut, numberofRequestFetch):

        self.processedReqeuests = queue.Queue()
        self.listenerTimeOut = listenerTimeOut
        self.requestQueueName = requestQueueName
        self.responseQueueName = responseQueueName
        self.visibilityTimeOut = visibilityTimeOut
        self.numberOfRequestFetched = numberofRequestFetch
        self.messageReceiverTimeout = 10
        self.requestsQueueUrl = None
        self.responseQueueUrl = None
        self.stopListner = False
        self.__init_queue_url()
        self.recurrentDeleteMessageJob = RepeatedTimer(30, self.delete_message)
        self.config = Util().get_config()
        self.darknetTargets = ['/home/ubuntu/distributedSurvellienceSystem/darknet','/home/ubuntu/distributedSurvellienceSystem/darknet2',
                                '/home/ubuntu/distributedSurvellienceSystem/darknet3'
                              ]       

        
    def __init_queue_url(self):
        sqs = boto3.client('sqs')  

        response = sqs.get_queue_url(QueueName=self.requestQueueName)
        if 'QueueUrl' in response:
            self.requestsQueueUrl = response['QueueUrl']
        response = sqs.get_queue_url(QueueName=self.responseQueueName)
        if 'QueueUrl' in response:
            self.responseQueueUrl = response['QueueUrl']
     
    def listener(self):
        sqs = boto3.client('sqs')    

        while True:
            logger.info("Fetching requests from the SQS")
            response = sqs.receive_message(
                        QueueUrl=self.requestsQueueUrl,
                        AttributeNames=[
                            'All'
                        ],
                        MaxNumberOfMessages=self.numberOfRequestFetched,
                        MessageAttributeNames=[
                            'All'
                        ],
                        VisibilityTimeout=self.visibilityTimeOut,
                        WaitTimeSeconds=self.messageReceiverTimeout
                    )
                
            if 'Messages' in response and len(response['Messages']) > 0:
                messages = response['Messages']
                logger.info("Received {0} requests to process".format(len(response['Messages'])))
                i = 0
                future = []
                with ThreadPoolExecutor(max_workers=self.numberOfRequestFetched) as executor:
                     for message in messages:
                        future = executor.submit(self.task, message, time.time(), self.darknetTargets[i])
                        i += 1
                if self.stopListner:
                    future.result()
                    logger.info("Stopping The listner")
                    break
            else:
                logger.info("No request receieved")
                if self.stopListner:
                    logger.info("Stopping The listner")
                    break
                time.sleep(10)
                continue
            time.sleep(self.listenerTimeOut)

    
    def delete_message(self):

        logger.info("Deleting the completed requests {0}".format(self.processedReqeuests.empty()))
        entries = []
        count = 0
        total_time = 0
        while not self.processedReqeuests.empty() and count < 10:
            entry = self.processedReqeuests.get()
            entries.append({
                'Id': entry[1],
                'ReceiptHandle': entry[0]
            })
            if entry[2]:
                 total_time += entry[2]
            count += 1

        logger.info("Meesage to be deleted {0}".format(entries))
        if count > 0:
            sqs = boto3.client('sqs')   
            response = sqs.delete_message_batch(
                QueueUrl=self.requestsQueueUrl,
                Entries=entries
            )
            avrageResponseTime = float(total_time/count)
            logger.info("Average Time - {0}".format(avrageResponseTime))
            logger.info("Deleted the completed requests Successfully")
        else:
            logger.info("No completed requests to delete")
        

    def task(self, message, receivedTime, targetDir):

        
        sqs = boto3.client('sqs')
        logger.info("Processing request {0}".format(message['MessageId']))
        d = dirname(dirname(abspath(__file__)))
        
        url = self.config.get('dev','VIDEO_URL')

        filename = message['MessageAttributes']['Filename']['StringValue']                                                                           
        filepath = os.path.join(d, "data", filename)
        self.__download_from_s3(filename, filepath)
        obj = self.__object_detect(filepath, targetDir)
        if obj is not None:
            bucket_name = self.config.get('dev','RESULT_BUCKET_NAME')
            objects = ', '.join(str(e) for e in obj)
            messageBody = " {0} ".format(objects)
            thread = threading.Thread(target=self.__upload_to_s3, args=(bucket_name, filename, objects))
            thread.start()
            response = sqs.send_message(
                        QueueUrl=self.responseQueueUrl,
                        MessageBody=messageBody,
                        MessageAttributes={
                            'correlationId': {
                                'StringValue': message['MessageId'],
                                'DataType': 'String'
                            },
                            'Filename': {
                                'StringValue':filename,
                                'DataType': 'String'
                            }
                            
                        },  
                        MessageDeduplicationId=message['MessageId'],
                        MessageGroupId="SurvellienceAppResponseGroup"
                    ) 
            curTime = time.time()
            self.processedReqeuests.put((message['ReceiptHandle'],message['MessageId'], curTime - receivedTime))
            logger.info("number of completed requests {0}".format(self.processedReqeuests.qsize()))    


    def __download_from_s3(self, key, path):

        s3 = boto3.resource('s3')
        bucket_name = self.config.get('dev','BUCKET_NAME')
        try:
            s3.Bucket(bucket_name).download_file(key, path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error("The object does not exist.")
            else:
                raise

    def __upload_to_s3(self,bukcet_name, key, content):
        s3 = boto3.resource("s3")
        s3.Object(bukcet_name, key).put(Body=content)


    def __object_detect(self,filepath, targetDir):
        
        weight_path = self.config.get('dev','WEIGHT_PATH')
        cmd = "xvfb-run ./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg "+ weight_path + " " + filepath + " -dont_show"
        logger.info(cmd)
 
        proc = subprocess.Popen([cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=targetDir)
        out, err = proc.communicate()
        
        exit_code = proc.returncode
        if exit_code:
            logger.error("Error While detection - ", err)
            return None
        logger.info("Successfully completed the detection - ", out)

        obj = set()
        out = out.decode(encoding="utf-8")
        lines = out.split("\n")
        for line in lines:
            if line == '' or line.split(":")[-1] == "":
                continue
            if line.split(":")[-1][-1] == "%":
                obj.add(line.split(":")[0])
        logger.info("Objects {0}".format(obj))
        return obj

    # def calculate_visibility_timeout():


