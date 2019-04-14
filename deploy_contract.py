from hashchain import ethereum
import json
import os

# get inputs
ETH_PRIVATE_KEY = os.environ.get('ETH_PRIVATE_KEY')
ETH_PUBLIC_KEY = os.environ.get('ETH_PUBLIC_KEY')
ETH_PROVIDER_URL = os.environ.get('ETH_PROVIDER_URL')

# deploy contract
print('Deploying contract ...')
contract = ethereum.EthContract()
contract.deploy(ETH_PUBLIC_KEY,ETH_PRIVATE_KEY,ETH_PROVIDER_URL)
contract.get_txn_receipt()
print('Contract deployed. Address: {}'.format(contract.address))

# save the contract address
with open('contract_interface.JSON','w+') as file:
    json.dump(dict(address=contract.address,abi = contract.abi),file)