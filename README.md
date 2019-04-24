# Hashchain-demo
This repos contains a script used to demo the [`hashchain` Python package](https://pypi.org/project/hashchain/).
This demo was meant to run on a Raspberry Pi 3 B+

**Logic:** 
1.  Deploy a smart contract on the Ethereum Blockchain if not already existing
2.  Get temperature sensor data
3.  Hash the record using `hashchain` Python package
4.  Store the record in a MongoDB Database
5.  Register the hash on the Ethereum Blockchain