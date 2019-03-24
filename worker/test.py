import boto3
def test():
    sqs = boto3.client('sqs')
    obj = set()
    objects = ', '.join(str(e) for e in obj)
    response = sqs.send_message(
           QueueUrl="https://sqs.us-west-2.amazonaws.com/122948564227/test3.fifo",
           MessageBody="fhfhd",
           MessageAttributes={
                            'correlationId': {
                                'StringValue': "dfsffdf",
                                'DataType': 'String'
                            },
                            'Filename': {
                                'StringValue':"test",
                                'DataType': 'String'
                            }
                            
                        },
           MessageDeduplicationId="sdadaflajpadl",
           MessageGroupId="SurvellienceAppResponseGroup"
         ) 




if __name__ == "__main__":
	test()