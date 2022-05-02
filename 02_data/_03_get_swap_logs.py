'''
query swap & liquidity activities from Uniswap v2
'''
import _01_constants as CT
import _02_utils as UT

import pandas as PD

def query_uniswap_swap() -> PD.DataFrame:
    '''
    query swap information from uniswap
    :param from_block: from which block to count
    :return: collected swap information
    '''
    # from_block = 14560000
    w3 = UT.init_web3()
    uniswap_factory_address = CT.ADDRESS['UniswapFactory']
    uniswap_factory_info = UT.init_contract(w3, uniswap_factory_address)
    uniswap_factory_contract = uniswap_factory_info['contract']
    list(uniswap_factory_contract.functions)

    trading_pair_address = uniswap_factory_contract.functions.getPair(CT.ADDRESS['WETH'], CT.ADDRESS['USDT']).call()
    trading_pair_info = UT.init_contract(w3, trading_pair_address)
    trading_pair_contract = trading_pair_info['contract']

    # list(trading_pair_contract.functions)
    # list(trading_pair_contract.events)

    step_blocks = 100
    # to_block = w3.eth.block_number
    to_block = CT.END_BLOCK

    from_block, end_block = CT.START_BLOCK, CT.START_BLOCK

    columns = ['BLOCK_NUMBER', 'BLOCK_TIME', 'TOKEN_A_IN', 'TOKEN_A_OUT', 'TOKEN_B_IN', 'TOKEN_B_OUT', 'SENDER', 'TX_HASH']
    total_collections = [[] for _ in columns]

    while end_block < to_block:
        begin_block = end_block + 1
        end_block = min(to_block, begin_block + step_blocks)
        print(f'\rSwap info, processing block {begin_block} -- {end_block}', end='')
        # query total swap logs during this block interval
        total_logs = trading_pair_contract.events.Swap.getLogs(fromBlock=begin_block, toBlock=end_block)
        for single_log in total_logs:
            # single_log = total_logs[0]
            block_number = single_log['blockNumber']
            block_timestamp = w3.eth.get_block(block_number)['timestamp']
            block_timestamp = int(block_timestamp) + 28800
            # block_time = datetime.datetime.fromtimestamp(block_timestamp)

            single_args = single_log['args']
            token_a_in = single_args['amount0In'] / 10 ** 18
            token_a_out = single_args['amount0Out'] / 10 ** 18
            token_b_in = single_args['amount1In'] / 10 ** 18
            token_b_out = single_args['amount1Out'] / 10 ** 18

            tx_hash = single_log['transactionHash'].hex()
            single_tx = w3.eth.get_transaction(tx_hash)
            sender = single_tx['from']

            # collect all values in one certain record
            single_values = [block_number, block_timestamp, token_a_in, token_a_out, token_b_in, token_b_out, sender, tx_hash]
            for value_index in range(len(single_values)):
                total_collections[value_index].append(single_values[value_index])


    temp_result = {}
    for column_index in range(len(columns)):
        temp_result.update({
            columns[column_index]: total_collections[column_index]
        })
    final_result = PD.DataFrame(temp_result)

    print()
    return final_result

def query_uniswap_liquidity() -> PD.DataFrame:
    '''
    query liquidity information from uniswap
    :param from_block: from which block to count
    :return: collected liquidity information
    '''
    # from_block = 14560000
    w3 = UT.init_web3()
    uniswap_factory_address = CT.ADDRESS['UniswapFactory']
    uniswap_factory_info = UT.init_contract(w3, uniswap_factory_address)
    uniswap_factory_contract = uniswap_factory_info['contract']
    list(uniswap_factory_contract.functions)

    trading_pair_address = uniswap_factory_contract.functions.getPair(CT.ADDRESS['WETH'], CT.ADDRESS['USDT']).call()
    trading_pair_info = UT.init_contract(w3, trading_pair_address)
    trading_pair_contract = trading_pair_info['contract']

    # list(trading_pair_contract.functions)
    # list(trading_pair_contract.events)

    step_blocks = 100
    # to_block = w3.eth.block_number
    to_block = CT.END_BLOCK + 2000

    from_block, end_block = CT.START_BLOCK, CT.START_BLOCK

    columns = ['BLOCK_NUMBER', 'BLOCK_TIME', 'TOKEN_A_AMOUNT', 'TOKEN_B_AMOUNT', 'EVENT', 'SENDER', 'TX_HASH']
    total_collections = [[] for _ in columns]

    while end_block < to_block:
        begin_block = end_block + 1
        end_block = min(to_block, begin_block + step_blocks)
        print(f'\rLiquidity info, processing block {begin_block} -- {end_block}', end='')
        # query total liquidity logs during this block interval
        mint_logs = trading_pair_contract.events.Mint.getLogs(fromBlock=begin_block, toBlock=end_block)
        burn_logs = trading_pair_contract.events.Burn.getLogs(fromBlock=begin_block, toBlock=end_block)
        total_logs = mint_logs + burn_logs
        for single_log in total_logs:
            # single_log = total_logs[0]
            block_number = single_log['blockNumber']
            block_timestamp = w3.eth.get_block(block_number)['timestamp']
            block_timestamp = int(block_timestamp) + 28800
            # block_time = datetime.datetime.fromtimestamp(block_timestamp)

            single_args = single_log['args']
            token_a_amount, token_b_amount = single_args['amount0'] / 10 ** 18, single_args['amount1'] / 10 ** 18

            tx_hash = single_log['transactionHash'].hex()
            single_tx = w3.eth.get_transaction(tx_hash)
            sender = single_tx['from']
            event = single_log['event']

            # collect all values in one certain record
            single_values = [block_number, block_timestamp, token_a_amount, token_b_amount, event, sender, tx_hash]
            for value_index in range(len(single_values)):
                total_collections[value_index].append(single_values[value_index])


    temp_result = {}
    for column_index in range(len(columns)):
        temp_result.update({
            columns[column_index]: total_collections[column_index]
        })
    final_result = PD.DataFrame(temp_result)

    print()
    return final_result

if __name__ == '__main__':

    swap_activities = query_uniswap_swap()
    liquidity_activities = query_uniswap_liquidity()

    swap_activities.to_csv('swap_activities.csv')
    liquidity_activities.to_csv('liquidity_activities.csv')