import asyncio
import aiohttp
import requests
from time import sleep
from google.cloud import pubsub_v1
import os

credential = os.environ['credential']
class data_process:   
	def __init__(self):
		self.temperature = []
		self.windDirection = []
		self.startTime = []
		self.shortForecast = []
		self.windSpeed = []
		self.project_id = os.environ['PROJECT_ID']
		self.topic_id = os.environ['TOPIC_ID']
		self.URL = os.environ['URL']
		# print("Hello")

	async def push_to_cloud(self,data:str)->None:
		#?cred
		publisher = pubsub_v1.PublisherClient()
		future = publisher.publish(f'projects/{self.project_id}/topics/{self.topic_id}',data)
		print(f'Published message id{future.result()}')
		return None

	async def fetch_refine(self)->dict:
		r = requests.get(url = self.URL)
		if 200<= r.status_code<400:
			dx = r.json()
			input = dx['properties']['periods'][0:4]
		else:
			raise Exception(f"Failed{r.status_code}:{r.text}")
		for element in input:
			self.temperature.append(element['temperature'])
			self.windDirection.append(element['windDirection'])
			self.shortForecast.append(element['shortForecast'])
			self.startTime.append(element['startTime'])
			self.windSpeed.append(int(element['windSpeed'].replace('mph','').replace('and','').strip()))
		return {'startTime':self.startTime,"temperature": self.temperature,"windDirection":\
		self.windDirection,"shortForecast":  self.shortForecast,\
		"windSpeed": self.windSpeed}


async def main():
    while(True):
        sx = data_process()
        data = await asyncio.gather(sx.fetch_refine())
        print(data)
        message = str(data).encode('utf-8')
        await asyncio.gather(sx.push_to_cloud(message))
        sleep(500)
	

if __name__ == '__main__':       
    asyncio.run(main())