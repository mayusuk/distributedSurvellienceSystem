import boto3
import random
import threading
import time
from cpuUtilizeMetric import CpuUtilizeMetric
from acceptableBacklogMetric import AcceptableBacklogMetric
import queue
import random
import requests
import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
print(sys.path)
from util.util import Util
from util.logger import Logger
logger = Logger().get_logger(__name__)


class autoscale:

    def __init__(self, min_instances, max_instances, warmuptime, prefix, healthEndpoint):
            self.prefix = prefix
            self.min_instances = min_instances
            self.max_instances = max_instances
            self.desired_instances = min_instances
            self.warmuptime = warmuptime
            self.list_instances = []
            self.stopped_instances = []
            self.random = str(random.randint(1, 9999))
            self.security_group, self.security_group_id = self.create_security_group()
           
            self._is_scaling = False
            self._lock = threading.Lock()
            self.instance_management_thread = threading.Thread(target=self.instance_management)
            self.watch_usage_thread = threading.Thread(target=self.watch_usage)
            self.healthEndpoint = healthEndpoint
            self.CpuUtilizeMaxMetric = CpuUtilizeMetric(60,5,"ScaleOut")
            self.CpuUtilizeMinMetric = CpuUtilizeMetric(30,10,"ScaleIn")     
            self.config = Util().get_config()
            self.AcceptableScaleOutBacklogMetric = AcceptableBacklogMetric(40, 40, self.config.get('dev','REQUESTS_SQS'), "ScaleOut")
            self.AcceptableScaleInBacklogMetric = AcceptableBacklogMetric(40, 40, self.config.get('dev','RESPONSE_SQS'), "ScaleIn")
            self.ScaleInMetric = [self.AcceptableScaleInBacklogMetric]
            self.ScaleOutMetric = [self.AcceptableScaleOutBacklogMetric]

            self.__init()
    
    def __init(self):

        d = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        filepath = os.path.join(d, "deployment", "survellience-app.sh")
        file = open(filepath, "r")

        self.user_data = file.read()

    def create_security_group(self):
        security_group = boto3.resource("ec2") 
        security_group_name = self.prefix + "-Security-group-" + self.random
        response = security_group.create_security_group(
            Description='Security Group for the Survellience App',
            GroupName=security_group_name,
            DryRun=False
        )
        if response: 
            response.authorize_ingress(
            DryRun=False,
            IpPermissions=[
                {
                    'FromPort': 22,
                    'ToPort': 22,
                    'IpProtocol': 'tcp',
                    'IpRanges': [
                        {
                            'CidrIp': "0.0.0.0/0",
                            'Description': 'SSH'
                        },
                    ]
                },
                {
                    'FromPort': 5000,
                    'ToPort': 5000,
                    'IpProtocol': 'tcp',
                    'IpRanges': [
                        {
                            'CidrIp': "0.0.0.0/0",
                            'Description': 'Health and Termination'
                        },
                    ]
                }
            ])

            return security_group_name, response.id
        return None, None

    def watch_usage(self):

        while True:
            self._lock.acquire() 
            total_instances = len(self.list_instances)
            if total_instances > 0: 
                scaleInResultQueue = queue.Queue()
                scaleOutResultQueue = queue.Queue()
                scaleOutThread = self.calculate(self.ScaleOutMetric, scaleOutResultQueue)   
                scaleInThread = self.calculate(self.ScaleInMetric, scaleInResultQueue)
                scaleInResult = self.__combineResult(scaleInThread, scaleInResultQueue)
                scaleOutResult = self.__combineResult(scaleOutThread, scaleOutResultQueue)


                if not self._is_scaling:
                    if True in scaleOutResult :                       
                        if self.desired_instances < self.max_instances:
                            logger.info("Scaling out the system....")
                            if self.desired_instances + self.AcceptableScaleOutBacklogMetric.step <= self.max_instances:
                                self.desired_instances += self.AcceptableScaleOutBacklogMetric.step
                            else:
                                self.desired_instances = self.max_instances
                    elif len(set(scaleInResult)) == 1 and True == scaleInResult[0]:
                        if self.desired_instances > self.min_instances:
                            logger.info("Scaling in the system....")
                            if self.desired_instances - self.AcceptableScaleInBacklogMetric.step <= self.min_instances:
                                self.desired_instances -= self.AcceptableScaleInBacklogMetric.step
                            else:
                                self.desired_instances = self.min_instances
            self._lock.release()

            time.sleep(60)

    def __combineResult(self, threads, queue):
        result = []
        for t in threads:
            t.join()
            result.append(queue.get())
        return result

    def calculate(self, metrics, queue):
        threads = []
        for metric in metrics:
            t = threading.Thread(target=metric.check_usage, args=(self.list_instances, len(self.list_instances), queue))
            threads.append(t)
            t.start()
        return threads


    def instance_management(self):

        while True:
            self._lock.acquire()
            if not self._is_scaling:
                if len(self.list_instances) < self.desired_instances:
                    self._is_scaling = True
                    logger.info("Desired Instances are more than runnning instances.Starting New Instances ....")
                    self.start_instance(self.desired_instances - len(self.list_instances))
                if len(self.list_instances) > self.desired_instances:
                    self._is_scaling = True
                    logger.info("Desired Instances are less than runnning instances.Terminating the Instances ....")
                    self.stop_instance(len(self.list_instances)- self.desired_instances)
                    self._is_scaling = False
            self._lock.release()
            time.sleep(80)

    def stop_instance(self, step):
        ec2 = boto3.client('ec2')
        logger.info("Using Step - {0}  for stopping instances".format(step))
        instance_ids = []
        i = 0
        for i in range(step):
            public_ip = self.list_instances[i].public_ip_address
            api = "http://{0}:5000/stop".format(public_ip)
            response = requests.get(api)
            print("Response - {0}".format(response.status_code))
            instance_ids.append(self.list_instances[i].instance_id)
        
        time.sleep(self.warmuptime)
        
        response = ec2.stop_instances(
                InstanceIds=instance_ids
        )

        self.stopped_instances.extend(self.list_instances[:step])

        self.list_instances = self.list_instances[step:]
            
       

    def start_instance(self,step):
        ec2 = boto3.resource('ec2')
        logger.info("Using Step - {0}  for starting instances".format(step))
        if len(self.stopped_instances) > 0:
            ec2_client = boto3.client('ec2')
            logger.info("Starting the stopped instances.")
            to_start_instance = self.stopped_instances[:step]
            self.stopped_instances = self.stopped_instances[step:]
            response = ec2_client.start_instances(
                InstanceIds=to_start_instance
            )
            if len(response['StartingInstances']) == len(to_start_instance):
                is_scaled = step==len(to_start_instance)
                t = threading.Timer(self.warmuptime, self.afterWarmUpPeriod, args=[to_start_instance, is_scaled])
                t.start()
                
            if len(to_start_instance) < step:
                step = step - len(to_start_instance)
            

        if step:

            response = ec2.create_instances(
                ImageId=self.config.get('dev','IMAGE_ID'),
                MaxCount=step,
                MinCount=step,
                InstanceType='t2.micro',
                KeyName=self.config.get('dev','SSH_KEY'),   
                Monitoring={
                    'Enabled': True
                },
                SecurityGroupIds=[
                    self.security_group_id,
                ],
                UserData=self.user_data                                                                                    
            )

            if len(response) == step:
                i = len(self.list_instances) + 1
                for instance in response:
                        instance_name = 'app-instance{0}'.format(i)
                        instance.create_tags(Tags=[{
                                'Key': 'name',
                                'Value': instance_name
                            }]) 
                        i += 1
                t = threading.Timer(self.warmuptime, self.afterWarmUpPeriod, args=[response, True])
                t.start() 

    def afterWarmUpPeriod(self, newInstances, is_scaled):
        if is_scaled:
            logger.info("System Is scaled")
        else:
            logger.info("Started the stopped Instances. System scaled partially")
        self._lock.acquire()
        self.list_instances.extend(newInstances)
        self._is_scaling = not is_scaled
        self._lock.release()

    def healthcheck(self, instances):

        logger.info("To be decide")