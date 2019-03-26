
from datetime import datetime, timedelta
import boto3
import math
import os, sys
import time
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
print(sys.path)

from util.logger import Logger

logger = Logger().get_logger(__name__)

class AcceptableBacklogMetric:

    def __init__(self,  averageResponseTime, acceptableLatency, queueName, type="ScaleIn"):
        self.acceptableBacklog = float(acceptableLatency/averageResponseTime)
        self.acceptableLatency = acceptableLatency
        self.averageResponseTime = averageResponseTime
        self.queueName = queueName
        self.validType = ["ScaleIn", "ScaleOut"]
        self.QueueUrl = None
        self.step = 1
        self.__init_url()
        if type in self.validType:
            self.type = type
        else:
            self.type = "ScaleOut"
        
    def __init_url(self):
        sqs = boto3.client('sqs')  

        response = sqs.get_queue_url(QueueName=self.queueName)
        if 'QueueUrl' in response:
            self.QueueUrl = response['QueueUrl']

    def check_usage(self, instances, totalInstances, queue):
        
        logger.info("Checking Usage")
        sqs = boto3.client('sqs')
        counter = 1
        if self.type == "ScaleIn":
                counter = 4
        
        avgNumberOfMessages = 0
        temp = counter 
        while temp > 0:
            
            response = sqs.get_queue_attributes(
                        QueueUrl=self.QueueUrl,
                        AttributeNames=[
                            'ApproximateNumberOfMessages',
                            'ApproximateNumberOfMessagesNotVisible'
                        ]
            )
            
            attributes = response['Attributes']
            meesagesInQueue= totalInstances
            if 'ApproximateNumberOfMessages' in attributes:
                meesagesInQueue = int(attributes['ApproximateNumberOfMessages'])
            avgNumberOfMessages += meesagesInQueue
            if self.type == "ScaleIn":
                avgNumberOfMessages += int(attributes['ApproximateNumberOfMessagesNotVisible'])
            temp -= 1
            time.sleep(2)

        avgNumberOfMessages = avgNumberOfMessages / counter
        logger.info("Current messages in queue {0} is {1} instances {2}".format( self.type, avgNumberOfMessages, totalInstances))
        backlogPerInstance = math.ceil(avgNumberOfMessages/totalInstances)
        remainingMessages = avgNumberOfMessages - (totalInstances * int(self.acceptableBacklog))
        scaleFactor = remainingMessages/int(self.acceptableBacklog)

        logger.info("Current Backlog per instance for {0} is {1}".format( self.type, backlogPerInstance))

        if self.type == "ScaleOut" and scaleFactor > 0:
            logger.info("Backlog per instances are more than desired. Step is {0}".format(math.ceil(scaleFactor))
            self.step = math.ceil(scaleFactor)
            queue.put(True)
        elif self.type == "ScaleIn" and scaleFactor < 0:
            logger.info("Backlog per instances are less than desired. Step is {0}".format(math.floor(scaleFactor))
            self.step = math.floor(scaleFactor)
            queue.put(True)
        else:
            logger.info("Backlog per instances  are normal")
            queue.put(False)
