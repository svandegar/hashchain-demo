from hashchain import records, ethereum
import time
import random
import pymongo
import os
import json
import asyncio

# Get inputs
with open('sensors_config.json') as file:
    sensors_config = json.load(file)

with open('contract_interface.JSON') as file:
    contract_interface = json.load(file)

ETH_PRIVATE_KEY = os.environ.get('ETH_PRIVATE_KEY')
ETH_PUBLIC_KEY = os.environ.get('ETH_PUBLIC_KEY')
ETH_PROVIDER_URL = os.environ.get('ETH_PROVIDER_URL')
MONGO_CONNECTION_STRING = os.environ.get('MONGO_CONNECTION_STRING')

eth_keys = ['timestamp', 'sensorId', 'value']

# Get mongo collection
client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
db = client['hashchain-demo']
sensors = db.sensors

# Build Ethereum connector
connector = ethereum.EthConnector(
    contract_abi=contract_interface['abi'],
    contract_address=contract_interface['address'],
    sender_public_key=ETH_PUBLIC_KEY,
    sender_private_key=ETH_PRIVATE_KEY,
    provider_url=ETH_PROVIDER_URL
)


class Simulator():

    def __init__(self, mongo_collection, ethereum_connector):
        self.eth_keys = eth_keys
        self.collection = mongo_collection
        self.connector = ethereum_connector

    def generate_data(self, sensor_id):
        temp = random.randrange(19, 21) + random.random()

        self.data = dict(timestamp=time.time(),
                         sensorId=sensor_id,
                         value=temp)

    def hash(self):
        try:
            last_record = self.collection.find({"sensorId": self.data['sensorId']}).sort([("timestamp", -1)])[0]
            last_record_hash = last_record['hash']
        except:
            last_record_hash = None

        self.record = records.Record(self.data, last_record_hash)

    def register_on_blockchain(self, eth_keys: list):
        try:
            eth_key = {x: self.record.get_content()[x] for x in eth_keys}.__str__()
            self.connector.record(str(self.record.get_content()), self.record.get_hash(), wait=True)

        except ValueError:
            print('second try')
            self.connector.record(str(self.record.get_content()), self.record.get_hash(), wait=True)

    async def run(self, frequency: int, sensor_id: str, eth_keys: list, max_iter=float('inf')):
        count = 0
        while count < max_iter:
            start = time.time()
            self.generate_data(sensor_id)
            self.hash()
            self.collection.insert_one(self.record.to_dict())
            self.register_on_blockchain(eth_keys)
            print(sensor_id, count)
            end = time.time()
            count += 1
            await asyncio.sleep(max(1, frequency - (end - start)))


# Run script
async def main():
    tasks = []
    simulator = Simulator(mongo_collection=sensors, ethereum_connector=connector)
    for sensor in sensors_config:
        task = asyncio.create_task(simulator.run(sensor['interval'], sensor['serialNumber'], eth_keys))
        tasks.append(task)
    await asyncio.gather(*tasks)


asyncio.run(main())
