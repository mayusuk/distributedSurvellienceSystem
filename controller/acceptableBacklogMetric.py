
from datetime import datetime, timedelta
import boto3
import math
import os, sys
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
        
        print("Checking Usage")
        sqs = boto3.client('sqs')

        response = sqs.get_queue_attributes(
                    QueueUrl=self.QueueUrl,
                    AttributeNames=[
                           'ApproximateNumberOfMessages'
                    ]
        )
        
        attributes = response['Attributes']
        meesagesInQueue= totalInstances
        if 'ApproximateNumberOfMessages' in attributes:
            meesagesInQueue = int(attributes['ApproximateNumberOfMessages'])

        logger.info("Current messages in queue {0} is {1} instances {2}".format( self.type, attributes['ApproximateNumberOfMessages'], totalInstances))
        backlogPerInstance = float(meesagesInQueue/totalInstances)
        
        logger.info("Current Backlog per instance for {0} is {1}".format( self.type, backlogPerInstance))

        if self.type == "ScaleOut" and int(backlogPerInstance - self.acceptableBacklog) > 0:
            logger.info("Backlog per instances are more than desired. Step is {0}".format(int(backlogPerInstance - self.acceptableBacklog)))
            self.step = int(backlogPerInstance - self.acceptableBacklog)
            queue.put(True)
        elif self.type == "ScaleIn" and int(math.ceil(self.acceptableBacklog - backlogPerInstance)) > 0:
            logger.info("Backlog per instances are less than desired. Step is {0}".format(int(math.ceil(self.acceptableBacklog - backlogPerInstance))))
            self.step = int(math.ceil(self.acceptableBacklog - backlogPerInstance))
            queue.put(True)
        else:
            logger.info("Backlog per instances  are normal")
            queue.put(False)
