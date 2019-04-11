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

        valid = records.verify(list(chain))

        # Verify the last hash on chain
        last_db_record = db_records[-1]
        eth_key = {x: last_db_record[x] for x in eth_keys}.__str__()
        last_eth_hash = ethereum_connector.getRecord(eth_key)

        if last_db_record['hash'] == last_eth_hash:
            return valid

        else:
            return False


    except ValueError as e:
        print(e)
        return False


def main():
    for sensor in sensors_config:
        result = verify(key='sensorId',
                        value=sensor['serialNumber'],
                        mongo_collection=sensors,
                        ethereum_connector=connector,
                        eth_keys=['timestamp', 'sensorId', 'value'])

        if result:
            print(f"Chain integrity for sensor {sensor['serialNumber']}: OK")
        else:
            print(f"Chain integrity for sensor {sensor}: NOK")


main()
