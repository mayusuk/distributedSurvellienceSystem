
from datetime import datetime, timedelta
import boto3

class CpuUtilizeMetric:

    def __init__(self, averageUtilization, time, type="ScaleIn"):
        self.averageUtilization = averageUtilization
        self.time = time
        self.validType = ["ScaleIn", "ScaleOut"]
        if type in self.validType:
            self.type = type
        else:
            self.type = "ScaleOut"

    def check_usage(self, instances, totalInstances, queue):
        
        cloud_watch = boto3.client('cloudwatch')

        total_average_load = 0.0
        now = datetime.utcnow()
        past = now - timedelta(minutes=self.time)
        future = now
        for instance in instances:
            results = cloud_watch.get_metric_statistics(
                    Namespace='AWS/EC2',
                    MetricName='CPUUtilization',
                    Dimensions=[{'Name': 'InstanceId', 'Value': instance.id}],
                    StartTime=past,
                    EndTime=future,
                    Period=300,
                    Statistics=['Average']
                )
        if results['Datapoints']:
            total_average_load += results['Datapoints'][0]['Average']
        average_load = total_average_load / totalInstances

        print("Current Average Load for {0} is {1}".format( self.type, average_load))

        if self.type == "ScaleOut" and average_load > self.averageUtilization:
            print("Total Average utilization Is more than desired")
            queue.put(True)
        elif self.type == "ScaleIn" and average_load < self.averageUtilization:
            print("Total Average utilization Is less than minimum desired")
            queue.put(True)
        else:
            print("Total Average utilization is normal")
            queue.put(False)
