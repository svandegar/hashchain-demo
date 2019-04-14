from hashchain import records, ethereum
from datetime import datetime
from gpiozero import LED
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


class Gateway():

    def __init__(self, mongo_collection, ethereum_connector):
        self.eth_keys = eth_keys
        self.collection = mongo_collection
        self.connector = ethereum_connector
        self.led_red = LED(10)
        self.led_green = LED(11)

    def get_sensors(self):
        self.sensors = []
        for file in os.listdir("/sys/bus/w1/devices/"):
            if file.startswith("28-"):
                self.sensors.append(file)

    def read_sensor(self, id):
        with open("/sys/bus/w1/devices/{}/w1_slave".format(id)) as file:
            text = file.read()
        secondline = text.split("\n")[1]
        temperature_data = secondline.split(" ")[9]
        temperature = float(temperature_data[2:]) / 1000
        self.data = dict(timestamp=datetime.now(),
                         sensorId=id,
                         value=temperature)
        print("Sensor: {} - Current temperature: {} C".format(id, temperature))

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
        self.get_sensors()
        count = 1
        while True:
            start = datetime.now()

            if len(self.sensors) == 0:
                print("No sensors found!")

            else:
                for sensor in self.sensors:

                    self.read_sensor(sensor)
                    self.hash()
                    self.collection.insert_one(self.record.to_dict())

                    if self.data['value'] >= threshold:
                        self.led_green.off()
                        self.led_red.on()
                        print('Threshold ({}) reached. Register record on blockchain'.format(threshold))
                        txn_hash = self.register_on_blockchain(eth_keys)
                        print("Transaction hash: {}".format(txn_hash))

                    elif count % validation_interval == 0:
                        print('Register record on blockchain every {} records'.format(validation_interval))
                        txn_hash = self.register_on_blockchain(eth_keys)
                        print("Transaction hash: {}".format(txn_hash))

                    else:
                        self.led_red.off()
                        self.led_green.on()

            end = datetime.now()
            count += 1
            time.sleep(max(0.01, frequency - (end - start).total_seconds()))


# Run script
def main():
    pi = Gateway(mongo_collection=data, ethereum_connector=connector)
    pi.run(frequency=1, eth_keys=eth_keys,validation_interval=60,threshold=30)




main()
