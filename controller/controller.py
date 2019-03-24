
from autoscale import autoscale
import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
print(sys.path)
from util.logger import Logger

logger = Logger().get_logger(__name__)

def controller():

    object = autoscale(max_instances=20, min_instances=1, warmuptime=120, prefix="test-1", healthEndpoint="/health")
    
    object.instance_management_thread.start()
    object.watch_usage_thread.start()

    object.instance_management_thread.join()
    object.watch_usage_thread.join()

        
if __name__ == "__main__":
    controller()