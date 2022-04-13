from web3 import Web3

# initialize web3 connection
w3 = Web3(Web3.WebsocketProvider('wss://rinkeby.infura.io/ws/v3/800054c370de4aa7907af5b45273c7fd'))

assert w3.isConnected(), '! the node is not connected !'

from_account = '0xd9ffd2E20C8f2e35312FF78707f1BA2349b398A2'

import configobj
private_key = configobj.ConfigObj('.env')['key']

to_account = '0xD1FF4b9e93E7c4DE84eAF004632819d49712ae5b'

# get the nonce.  Prevents one from sending the transaction twice
nonce = w3.eth.getTransactionCount(from_account)

# build a transaction in a dictionary
tx = {
    'nonce': nonce,
    'to': to_account,
    'value': w3.toWei(0.1, 'ether'),
    'gas': 2000000,
    'gasPrice': w3.toWei('100', 'gwei')
}

#sign the transaction
signed_tx = w3.eth.account.sign_transaction(tx, private_key)

#send transaction
tx_hash = w3.eth.sendRawTransaction(signed_tx.rawTransaction)

#get transaction hash
print(w3.toHex(tx_hash))