'''
update naughty pools
since the naughty factory will be used only once, so this file would be refined later on
'''
import constants as CT
import utils as UT

import pandas as PD

def query_naughty_token_address(naughty_factory_contract):
    # list(contracts['NaughtyFactory'].functions)
    naughty_tokens_count = naughty_factory_contract.functions._nextId().call()
    total_naughty_tokens, total_naughty_pairs = [], []
    for naughty_index in range(naughty_tokens_count):
        try:
            single_naughty_token = naughty_factory_contract.functions.allTokens(naughty_index).call()
            single_naughty_pair = naughty_factory_contract.functions.allPairs(naughty_index).call()
            total_naughty_tokens.append(single_naughty_token)
            total_naughty_pairs.append(single_naughty_pair)
        except:
            continue
    return total_naughty_tokens, total_naughty_pairs

def fetch_all_naughty_tokens():
    w3 = UT.init_web3()
    naughty_factory_info = UT.init_contract(w3, CT.ADDRESS['NaughtyFactory'])
    naughty_factory_contract = naughty_factory_info['contract']
    total_naughty_tokens, total_naughty_pairs = query_naughty_token_address(naughty_factory_contract)
    all_abi = UT.load_abi()

    all_columns = ['NAUGHTY_FACTORY', 'NAUGHTY_ROUTER', 'NAUGHTY_TOKEN', 'NAUGHTY_PAIR', 'SYMBOL']
    total_values = [[] for _ in all_columns]

    for naughty_token_index in range(len(total_naughty_tokens)):
        single_token_contract = w3.eth.contract(address=w3.toChecksumAddress(total_naughty_tokens[naughty_token_index]), abi=all_abi['NPPolicyToken'])
        token_name = single_token_contract.functions.symbol().call()
        single_values = [CT.ADDRESS['NaughtyFactory'], CT.ADDRESS['NaughtyRouter'], total_naughty_pairs[naughty_token_index], total_naughty_pairs[naughty_token_index], token_name]
        for value_index in range(len(single_values)):
            total_values[value_index].append(single_values[value_index])

    temp_result = {}
    for column_index in range(len(all_columns)):
        temp_result.update({
            all_columns[column_index]: total_values[column_index]
        })
    final_result = PD.DataFrame(temp_result)
    return final_result

def upload_naughty_tokens():
    table_name = 'naughty_token_overview'
    newest_result = fetch_all_naughty_tokens()
    UT.upload_sql_records(table_name, newest_result, True)

if __name__ == '__main__':
    upload_naughty_tokens()