from hashchain import records, ethereum
from datetime import datetime
import random
import pymongo
import os
import json
import time

# Get inputs
ETH_PRIVATE_KEY = os.environ.get('ETH_PRIVATE_KEY')
ETH_PUBLIC_KEY = os.environ.get('ETH_PUBLIC_KEY')
ETH_PROVIDER_URL = os.environ.get('ETH_PROVIDER_URL')
MONGO_CONNECTION_STRING = os.environ.get('MONGO_CONNECTION_STRING')

eth_keys = ['sensorId']

# Simulate inputs
simulation = []
for _ in range(1000):
    simulation.append(round(29.899 + (random.random() / 10), 2))
simulation[30] = 4.01

sensors =["28-041780c449ff"]

# Get Eth contract interface. Deploy one if not existing
try:
    with open('contract_interface.JSON') as file:
        contract_interface = json.load(file)
except FileNotFoundError:
    print('Deploying contract ...')
    contract = ethereum.EthContract()
    contract.deploy(ETH_PUBLIC_KEY, ETH_PRIVATE_KEY, ETH_PROVIDER_URL)
    contract.get_txn_receipt()
    print('Contract deployed. Address: {}'.format(contract.address))
    with open('contract_interface.JSON', 'w+') as file:
        json.dump(dict(address=contract.address, abi=contract.abi), file)


# Get mongo collection
client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
db = client['hashchain-demo']
data = db.data

data.delete_many({})

# Build Ethereum connector
connector = ethereum.EthConnector(
    contract_abi=contract_interface['abi'],
    contract_address=contract_interface['address'],
    sender_public_key=ETH_PUBLIC_KEY,
    sender_private_key=ETH_PRIVATE_KEY,
    provider_url=ETH_PROVIDER_URL
)


class Simulator():

    def __init__(self, mongo_collection, ethereum_connector, sensors):
        self.eth_keys = eth_keys
        self.collection = mongo_collection
        self.connector = ethereum_connector
        self.sensors = sensors


    def generate_data(self, sensor_id, i):
        self.data = dict(timestamp=datetime.now().replace(microsecond=0),
                         sensorId=sensor_id,
                         value=simulation[i])
        print("Sensor: {} - Current temperature: {} C".format(sensor_id,simulation[i]))

    def hash(self):
        try:
            last_record = self.collection.find({"sensorId": self.data['sensorId']}).sort([("timestamp", -1)])[0]
            last_record_hash = last_record['hash']
        except:
            last_record_hash = None

        self.record = records.Record(self.data, last_record_hash)

    def register_on_blockchain(self, eth_keys: list):
        eth_key = {x: self.record.get_content()[x] for x in eth_keys}.__str__()
        return self.connector.record(eth_key, self.record.get_hash(), wait=True).hex()

    def run(self,
                  frequency: int,
                  eth_keys: list,
                  validation_interval: int,
                  threshold=float('inf')):

        count = 1
        while True:
            start = datetime.now()

            if len(self.sensors) == 0:
                print("No sensors found!")

            else:
                for sensor in self.sensors:

                    self.generate_data(sensor,count)
                    self.hash()
                    self.collection.insert_one(self.record.to_dict())

                    if self.data['value'] >= threshold:
                        print('Threshold ({}) reached. Register record on blockchain'.format(threshold))
                        txn_hash = self.register_on_blockchain(eth_keys)
                        print("Transaction hash: {}".format(txn_hash))

                    elif count % validation_interval == 0:
                        print('Register record on blockchain every {} records'.format(validation_interval))
                        txn_hash = self.register_on_blockchain(eth_keys)
                        print("Transaction hash: {}".format(txn_hash))

            end = datetime.now()
            count += 1
            time.sleep(max(0.01, frequency - (end - start).total_seconds()))


# Run script
def main():
    print('Start reading sensors...')
    pi = Simulator(mongo_collection=data, ethereum_connector=connector, sensors=sensors)
    pi.run(frequency=1, eth_keys=eth_keys,validation_interval=60,threshold=30)


try:
    main()
except KeyboardInterrupt:
    print('\nScript ended by user')
