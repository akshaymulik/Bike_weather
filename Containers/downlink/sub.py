from google.cloud import pubsub_v1
import os
from pandas import DataFrame
from concurrent.futures import TimeoutError
from google.cloud.storage import Client
"""
credits https://gitlab.com/ryanlogsdon/pubsub-for-python/-/blob/main/subscriber.py
"""
timeout = 10
bucket_name = os.environ['bucket_name']
credential = os.environ['credential']
subscriber = pubsub_v1.SubscriberClient()
subscription_path = os.environ['subscription_path']

def callback(message):
    # print(f'Received message: {message}')
    print(f'data: {message.data}')
    # print(message.data.decode('utf-8'))
    c = eval(message.data.decode('utf-8'))
    data = c[0]
    fname = data['startTime'][0]
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{fname}.csv")
    blob.upload_from_string(data=DataFrame(data).to_csv(index=False),content_type="text/csv")
    message.ack()           


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f'Listening for messages on {subscription_path}')


with subscriber:                                                # wrap subscriber in a 'with' block to automatically call close() when done
    try:
        # streaming_pull_future.result(timeout=timeout)
        streaming_pull_future.result()                          # going without a timeout will wait & block indefinitely
    except TimeoutError:
        streaming_pull_future.cancel()                          # trigger the shutdown
        streaming_pull_future.result()                
                        # block until the shutdown is complete


# class subscriber_to_bucket:
# 	def __init__(self):
# 		self.data = {}
	
# 	def get_message(self):
		

# # 	# def upload_to_store(self,)
# # if __name__ == '__main__':
# # 	acx = subscriber_to_bucket()
# # 	acx.get_message()
# # 	print(acx.data)