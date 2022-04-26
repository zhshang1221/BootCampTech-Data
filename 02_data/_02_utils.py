import _01_constants as CT

import requests
from web3 import Web3
from web3.middleware import geth_poa_middleware

def init_web3():
    '''
    initialize web3 instance
    :return: web3 instance
    '''
    w3 = Web3(Web3.WebsocketProvider('wss://mainnet.infura.io/ws/v3/800054c370de4aa7907af5b45273c7fd'))
    assert w3.isConnected(), '!! Attention !! The node is not connected !!!'
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w3

def get_abi(address):
    '''
    get ABI of certain smart contract, marked by its address
    :param address: address of certain smart contract
    :return: abi for this contract
    '''
    # address = '0x60aE616a2155Ee3d9A68541Ba4544862310933d4'
    MAX_TRIAL = 5
    error_count = 0
    api_key = 'UCJ24GP9ICCR28QNPDNCXZ27VHWIG442F6'
    url = f'https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={api_key}'
    while error_count < MAX_TRIAL:
        try:
            # print('Retrieving:', url.format(address=address, api_key=api_key))
            response = requests.get(url.format(address=address, api_key=api_key))
            if response.status_code == 200:
                json_response = response.json()
                if json_response['status'] == '1': # Valid data returned
                    return json_response['result']
                else:
                    error_count += 1
            else:
                error_count += 1
        except Exception as e:
            print('Error happened when asking for ABI', e)
            error_count += 1
    else:
        return ''

def init_contract(w3, address) -> dict:
    '''
    initialize instance for certain smart contract
    :param w3: web3 instance
    :param address: address for this contract
    :return: dict consisting of
        -- address: smart contract address
        -- abi: smart contract abi
        -- contract: smart contract instance
    '''
    # address = '0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10'
    abi_info = get_abi(address)
    single_contract = w3.eth.contract(address=w3.toChecksumAddress(address), abi=abi_info)
    return {
        'address': address,
        'abi': abi_info,
        'contract': single_contract
    }

if __name__ == '__main__':
    w3 = init_web3()
    uniswap_factory_address = CT.ADDRESS['UniswapFactory']
    uniswap_factory_info = init_contract(w3, uniswap_factory_address)
    uniswap_factory_contract = uniswap_factory_info['contract']
    list(uniswap_factory_contract.functions)

    trading_pair_address = uniswap_factory_contract.functions.getPair(CT.ADDRESS['WETH'], CT.ADDRESS['USDT']).call()
    print('This is WETH-USDT address:', trading_pair_address)