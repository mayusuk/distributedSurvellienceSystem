import boto3
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
class SQSWorker:



    def __init__(self, listenerTimeOut, requestQueueName, responseQueueName, visibilityTimeOut):

        self.processedReqeuests = queue.Queue()
        self.listenerTimeOut = listenerTimeOut
        self.requestQueueName = requestQueueName
        self.responseQueueName = responseQueueName
        self.visibilityTimeOut = visibilityTimeOut
        self.messageReceiverTimeout = 10
        self.requestsQueueUrl = None
        self.responseQueueUrl = None
        self.__init_queue_url()
        self.recurrentDeleteMessageJob = RepeatedTimer(60, self.delete_message)
        self.recurrentDeleteMessageJob.start()
        self.config = Util().get_config()
        self.darknetTargets = ['/home/ubuntu/Project/darknet','/home/ubuntu/Project/darknet2','/home/ubuntu/Project/darknet3','/home/ubuntu/Project/darknet4',
                                '/home/ubuntu/Project/darknet5', '/home/ubuntu/Project/darknet6', '/home/ubuntu/Project/darknet7', '/home/ubuntu/Project/darknet8',
                                '/home/ubuntu/Project/darknet9'
                                ]
        
    def __init_queue_url(self):
        sqs = boto3.client('sqs')  

        response = sqs.get_queue_url(QueueName=self.requestQueueName)
        if 'QueueUrl' in response:
            self.requestsQueueUrl = response['QueueUrl']
        # response = sqs.get_queue_url(QueueName=self.responseQueueName)
        # if 'QueueUrl' in response:
        #     self.responseQueueUrl = response['QueueUrl']
     
    def listener(self):
        sqs = boto3.client('sqs')    

        while True:
            print("Fetching requests from the SQS")
            response = sqs.receive_message(
                        QueueUrl=self.requestsQueueUrl,
                        AttributeNames=[
                            'All'
                        ],
                        MaxNumberOfMessages=10,
                        MessageAttributeNames=[
                            'All'
                        ],
                        VisibilityTimeout=self.visibilityTimeOut,
                        WaitTimeSeconds=self.messageReceiverTimeout
                    )
                
            if 'Messages' in response and len(response['Messages']) > 0:
                messages = response['Messages']
                print("Received {0} requests to process".format(len(response['Messages'])))
                i = 0
                with ThreadPoolExecutor(max_workers=2) as executor:
                     for message in messages:
                        future = executor.submit(self.task, message, time.time(), self.darknetTargets[i])
                        i += 1
            else:
                print("No request receieved")
                time.sleep(10)
                continue
            time.sleep(self.listenerTimeOut)

    def delete_message(self):

        print("Deleting the completed requests {0}".format(self.processedReqeuests.empty()))
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

        print("Entries", entries)
        if count > 0:
            sqs = boto3.client('sqs')   
            response = sqs.delete_message_batch(
                QueueUrl=self.requestsQueueUrl,
                Entries=entries
            )
            avrageResponseTime = float(total_time/count)
            print("Average Time - {0}".format(avrageResponseTime))
            print("Deleted the completed requests Successfully")
        else:
            print("No completed requests to delete")

        # if 'Failed' in response and len(response['Failed']) > 0:
        #     for failed in response['Failed']:
        #         self.processedReqeuests.put((failed[''], failed['']))
        

    def task(self, message, receivedTime, targetDir):

        curTime = time.time()
        sqs = boto3.client('sqs')
        print("Processing request {0}".format(message['MessageId']))
        d = dirname(dirname(abspath(__file__)))
        
        url = self.config.get('dev','VIDEO_URL')

        # remotefile = urllib.request.urlopen(url)
        # blah = remotefile.info()['Content-Disposition']
        # _, params = cgi.parse_header(blah)

        filename = message['MessageId']                                                                             
        filepath = os.path.join(d, "data", message['MessageId'])
        result = urllib.request.urlretrieve(url, filepath)
        print(filepath)
        obj = self.__object_detect(filepath, targetDir)
        if obj is not None:
            bucket_name = self.config.get('dev','BUCKET_NAME')
            thread = threading.Thread(target=self.__upload_to_s3, args=(bucket_name, filename, list(obj)))
            thread.start()
            self.processedReqeuests.put((message['ReceiptHandle'],message['MessageId'], curTime - receivedTime))
            print("number of completed requests {0}".format(self.processedReqeuests.qsize()))    
         

    def __upload_to_s3(self,bukcet_name, key, content):
        s3 = boto3.resource("s3")
        s3.Object(bukcet_name, key).put(Body=str(content))

    def __object_detect(self,filepath, targetDir):
        
        weight_path = self.config.get('dev','WEIGHT_PATH')
        cmd = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg "+ weight_path + " " + filepath + " -dont_show"
        print(cmd)
 
        proc = subprocess.Popen([cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=targetDir)
        out, err = proc.communicate()
        
        exit_code = proc.returncode
        if exit_code:
            print("Error While detection - ", err)
            return None
        print("Successfully completed the detection - ", out)

        obj = set()
        lines = out.split("\n")
        for line in lines:
            if line == '' or line.split(":")[-1] == "":
                continue
            if line.split(":")[-1][-2] == "%":
                obj.add(line.split(":")[0])
        print("Objects {0}".format(obj))
        return obj

    # def calculate_visibility_timeout():



if __name__ == '__main__':

    sqsworker = SQSWorker(60,"test2.fifo", "response.fifo", 240)
    sqsWorkerThread = threading.Thread(target=sqsworker.listener)
    sqsWorkerThread.start()
    sqsWorkerThread.join()