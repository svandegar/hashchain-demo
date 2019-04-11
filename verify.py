from hashchain import records, ethereum
import pymongo
import os
import json

ETH_PRIVATE_KEY = os.environ.get('ETH_PRIVATE_KEY')
ETH_PUBLIC_KEY = os.environ.get('ETH_PUBLIC_KEY')
ETH_PROVIDER_URL = os.environ.get('ETH_PROVIDER_URL')
MONGO_CONNECTION_STRING = os.environ.get('MONGO_CONNECTION_STRING')

""" Verify the chain integrity """

with open('sensors_config.json') as file:
    sensors_config = json.load(file)

with open('contract_interface.JSON') as file:
    contract_interface = json.load(file)

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


def verify(key, value, mongo_collection, ethereum_connector, eth_keys: list):
    try:
        chain = mongo_collection.find({key: value},
                                      {"_id": 0}).sort([("timestamp", 1)])

        db_records = list(chain)

        # Verify the haschain from the DB
        if records.verify(list(chain)):

            # Verify the last hash on chain
            last_db_record = db_records[-1]
            eth_key = {x: last_db_record[x] for x in eth_keys}
            # db_record = mongo_collection.find(eth_key)
            # db_hash = db_record[0]['hash']
            last_eth_hash = ethereum_connector.get_record(eth_key.__str__())
            last_valid_db_record = mongo_collection.find_one({key: value,'hash':last_eth_hash},
                                                         {"_id": 0})
            return last_valid_db_record

        else:
            return False


    except ValueError as e:
        print(e)
        return False


def main():
    for sensor in sensors_config:
        last_valid_record = verify(key='sensorId',
                        value=sensor['serialNumber'],
                        mongo_collection=sensors,
                        ethereum_connector=connector,
                        eth_keys=['sensorId'])

        if last_valid_record:
            print(f"Chain integrity for sensor {sensor['serialNumber']}: OK.")
            print(f"Last record registered on blockchain: {json.dumps(last_valid_record, indent=4)}")
            print('===========================================================')

        else:
            print(f"Chain integrity for sensor {sensor['serialNumber']}: NOK")
            print(f"Last record registered on blockchain: {json.dumps(last_valid_record, indent=4)}")
            print('===========================================================')
main()
