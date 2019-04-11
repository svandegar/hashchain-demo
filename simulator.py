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

eth_keys = ['sensorId']

# Simulate inputs
simulation = []
for _ in range(1000):
    simulation.append(round(3.899 + (random.random() / 10), 2))
simulation[30] = 4.01

# Get mongo collection
client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
db = client['hashchain-demo']
sensors = db.sensors

sensors.delete_many({})

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

    def generate_data(self, sensor_id, i):
        self.data = dict(timestamp=time.time(),
                         sensorId=sensor_id,
                         value=simulation[i])

    def hash(self):
        try:
            last_record = self.collection.find({"sensorId": self.data['sensorId']}).sort([("timestamp", -1)])[0]
            last_record_hash = last_record['hash']
        except:
            last_record_hash = None

        self.record = records.Record(self.data, last_record_hash)

    def register_on_blockchain(self, eth_keys: list):
        eth_key = {x: self.record.get_content()[x] for x in eth_keys}.__str__()
        try:
            return self.connector.record(eth_key, self.record.get_hash(), wait=True).hex()

        except ValueError:
            return self.connector.record(eth_key, self.record.get_hash(), wait=True).hex()

    async def run(self,
                  frequency: int,
                  sensor_id: str,
                  eth_keys: list,
                  validation_interval: int,
                  max_iter=float('inf'),
                  threshold=float('inf')):
        count = 1
        while count < max_iter:
            start = time.time()
            self.generate_data(sensor_id, count)
            print(f"{sensor_id}: value = {self.data['value']}")
            self.hash()
            self.collection.insert_one(self.record.to_dict())

            if self.data['value'] >= threshold:
                print(f'Threshold ({threshold}) reached. Register record on blockchain')
                txn_hash = self.register_on_blockchain(eth_keys)
                print(f"Transaction hash: {txn_hash}")

            elif count % validation_interval == 0:
                print(f'Register record on blockchain')
                txn_hash = self.register_on_blockchain(eth_keys)
                print(f"Transaction hash: {txn_hash}")

            end = time.time()
            count += 1
            await asyncio.sleep(max(0.01, frequency - (end - start)))


# Run script
async def main():
    tasks = []
    simulator = Simulator(mongo_collection=sensors, ethereum_connector=connector)
    for sensor in sensors_config:
        task = asyncio.create_task(simulator.run(sensor['interval'],
                                                 sensor['serialNumber'],
                                                 eth_keys,
                                                 threshold=4,
                                                 validation_interval=60))
        tasks.append(task)
    await asyncio.gather(*tasks)


asyncio.run(main())
