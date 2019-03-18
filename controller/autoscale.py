
import boto3
import random
import threading
import time
from cpuUtilizeMetric import CpuUtilizeMetric
import queue

class autoscale:

    def __init__(self, min_instances, max_instances, warmuptime, prefix, healthEndpoint):
            self.prefix = prefix
            self.min_instances = min_instances
            self.max_instances = max_instances
            self.desired_instances = min_instances
            self.warmuptime = warmuptime
            self.list_instances = []
            self.random = str(random.randint(1, 9999))
            self.security_group, self.security_group_id = self.create_security_group()
           
            self._is_scaling = False
            self._lock = threading.Lock()
            self.instance_management_thread = threading.Thread(target=self.instance_management)
            self.watch_usage_thread = threading.Thread(target=self.watch_usage)
            self.healthEndpoint = healthEndpoint
            self.CpuUtilizeMaxMetric = CpuUtilizeMetric(60,5,"ScaleOut")
            self.CpuUtilizeMinMetric = CpuUtilizeMetric(30,10,"ScaleIn")
            self.ScaleInMetric = [self.CpuUtilizeMinMetric]
            self.ScaleOutMetric = [self.CpuUtilizeMaxMetric]

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
                    'FromPort': 8080,
                    'ToPort': 8080,
                    'IpProtocol': 'tcp',
                    'IpRanges': [
                        {
                            'CidrIp': "0.0.0.0/0",
                            'Description': 'WebServer'
                        },
                    ]
                },
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
                            print("Scaling out the system....")
                            self.desired_instances += 1
                    elif len(set(scaleInResult)) == 1 and True == scaleInResult[0]:
                        if self.desired_instances > self.min_instances:
                            print("Scaling in the system....")
                            self.desired_instances -= 1
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
                    print("Desired Instances are more than runnning instances.Starting New Instances ....")
                    self.start_instance(self.desired_instances - len(self.list_instances))
                if len(self.list_instances) > self.desired_instances:
                    self._is_scaling = True
                    print("Desired Instances are less than runnning instances.Terminating the Instances ....")
                    self.stop_instance(len(self.list_instances)- self.desired_instances)
                    self._is_scaling = False
            self._lock.release()
       
    def stop_instance(self, step):
    
        print("To be Implemented")
        self.list_instances.pop()      


    def start_instance(self,step):
        ec2 = boto3.resource('ec2')

        response = ec2.create_instances(
            ImageId='ami-0db581a45ff3698e0',
            MaxCount=step,
            MinCount=1,
            InstanceType='t2.nano',
            KeyName='auto-scaling',   
            Monitoring={
                'Enabled': True
            },
            SecurityGroupIds=[
                self.security_group_id,
            ]                                                                                     
        )

        if len(response) == step:
            t = threading.Timer(self.warmuptime, self.afterWarmUpPeriod, args=[response])
            t.start() 

    def afterWarmUpPeriod(self, newInstances):

        print("System Is scaled")
        self._lock.acquire()
        self.list_instances.extend(newInstances)
        self._is_scaling = False
        self._lock.release()

    def healthcheck(self, instances):

        print("To be decide")