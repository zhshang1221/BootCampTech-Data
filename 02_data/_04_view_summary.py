import _01_constants as CT
import _02_utils as UT

import numpy as NP
import pandas as PD

import datetime

import requests, time, warnings
warnings.filterwarnings('ignore')
from bs4 import BeautifulSoup as BS

from pyecharts import Line, Grid

def generate_swap_summary() -> PD.DataFrame:
    columns_info = ['block_number', 'block_time', 'token_a_in', 'token_a_out', 'token_b_in', 'token_b_out', 'sender', 'tx_hash']

    swap_data = PD.read_csv('swap_details.csv')
    del swap_data[swap_data.columns[0]]
    swap_data.columns = columns_info

    swap_data['block_time'] = PD.to_datetime(swap_data['block_time'])

    real_start, real_end = NP.min(swap_data['block_time'].values), NP.max(swap_data['block_time'].values)
    real_start, real_end = datetime.datetime.fromtimestamp(int(real_start) / 1e9 - 28800), datetime.datetime.fromtimestamp(int(real_end) / 1e9 - 28800)

    output_columns = ['date', 'sell_count', 'sell_volume', 'sell_price', 'buy_count', 'buy_volume', 'buy_price', 'estimate_fee', 'price_start', 'price_end']
    total_values = [[] for _ in output_columns]

    date_begin = real_start
    date_end = date_begin + datetime.timedelta(days=1)
    while date_begin <= real_end:
        # print(f'Processing date {str(date_begin)[: 10]}')
        single_date = str(date_begin)[: 10]
        valid_records = swap_data.loc[swap_data['block_time'] >= date_begin]
        valid_records = valid_records.loc[valid_records['block_time'] < date_end]

        # price for the first swap & last swap
        price_start = (valid_records['token_b_in'].values[0] + valid_records['token_b_out'].values[0]) / (valid_records['token_a_in'].values[0] + valid_records['token_a_out'].values[0])
        price_end = (valid_records['token_b_in'].values[-1] + valid_records['token_b_out'].values[-1]) / (valid_records['token_a_in'].values[-1] + valid_records['token_a_out'].values[-1])

        swap_in_records = valid_records.loc[valid_records['token_a_out'] == 0.]
        swap_out_records = valid_records.loc[valid_records['token_a_in'] == 0.]

        # swap in, means sell the insurance token

        single_in_count = len(swap_in_records)
        single_in_volume = sum(swap_in_records['token_a_in'])
        if single_in_volume > 0:
            single_in_price = sum(swap_in_records['token_b_out']) / single_in_volume
        else:
            single_in_price = 0.

        # swap out, means buy insurance token from swap pool
        single_out_count = len(swap_out_records)
        single_out_volume = sum(swap_out_records['token_a_out'])
        if single_out_volume > 0:
            single_out_price = sum(swap_out_records['token_b_in']) / single_out_volume
        else:
            single_out_price = 0.

        estimate_fee = sum(valid_records['token_b_out']) + sum(valid_records['token_b_in'])

        single_values = [single_date, single_in_count, single_in_volume, single_in_price, single_out_count, single_out_volume, single_out_price, estimate_fee, price_start, price_end]
        for value_index in range(len(single_values)):
            total_values[value_index].append(single_values[value_index])

        date_begin = date_end
        date_end = date_begin + datetime.timedelta(days=1)

    temp_result = {}
    for column_index in range(len(output_columns)):
        temp_result.update({
            output_columns[column_index]: total_values[column_index]
        })
    final_result = PD.DataFrame(temp_result)
    final_result.to_csv(CT.STORE_PATH + 'swap_info.csv')
    return final_result

def generate_liquidity_summary() -> PD.DataFrame:

    columns_info = ['block_number', 'block_time', 'token_a_amount', 'token_b_amount', 'operation', 'sender', 'tx_hash']

    liquidity_data = PD.read_csv('liquidity_details.csv')
    del liquidity_data[liquidity_data.columns[0]]
    liquidity_data.columns = columns_info

    liquidity_data['block_time'] = PD.to_datetime(liquidity_data['block_time'])

    real_start, real_end = NP.min(liquidity_data['block_time'].values), NP.max(liquidity_data['block_time'].values)
    real_start, real_end = datetime.datetime.fromtimestamp(int(real_start) / 1e9 - 28800), datetime.datetime.fromtimestamp(int(real_end) / 1e9 - 28800)

    output_columns = ['date', 'stake_count', 'stake_volume', 'unstake_count', 'unstake_volume', 'reserve']
    total_values = [[] for _ in output_columns]

    date_begin = real_start
    date_end = date_begin + datetime.timedelta(days=1)
    initial_reserve = 0.
    while date_begin <= real_end:
        # print(f'Processing date {str(date_begin)[: 10]}')
        single_date = str(date_begin)[: 10]
        valid_records = liquidity_data.loc[liquidity_data['block_time'] >= date_begin]
        valid_records = valid_records.loc[valid_records['block_time'] < date_end]

        liquidity_in_records = valid_records.loc[valid_records['operation'] == 'Mint']
        liquidity_out_records = valid_records.loc[valid_records['operation'] == 'Burn']

        # liquidity in, means sell the insurance token

        single_stake_count = len(liquidity_in_records)
        single_stake_volume = sum(liquidity_in_records['token_a_amount'])

        # liquidity out, means buy insurance token from liquidity pool
        single_unstake_count = len(liquidity_out_records)
        single_unstake_volume = sum(liquidity_out_records['token_a_amount'])

        initial_reserve = initial_reserve + single_stake_volume - single_unstake_volume

        single_values = [single_date, single_stake_count, single_stake_volume, single_unstake_count, single_unstake_volume, initial_reserve]
        for value_index in range(len(single_values)):
            total_values[value_index].append(single_values[value_index])

        date_begin = date_end
        date_end = date_begin + datetime.timedelta(days=1)

    temp_result = {}
    for column_index in range(len(output_columns)):
        temp_result.update({
            output_columns[column_index]: total_values[column_index]
        })
    final_result = PD.DataFrame(temp_result)
    final_result.to_csv(CT.STORE_PATH + 'liquidity_info.csv')
    return final_result

def generate_pnl():
    swap_info = generate_swap_summary()
    liquidity_info = generate_liquidity_summary()
    #  = 'AVAX_94.0_L_2104'
    assert len(swap_info) == len(liquidity_info), 'Swap does not match with Liquidity'

    output_columns = ['date', 'estimate_income', 'estimate_il', 'estimate_mining']
    total_values = [[] for _ in output_columns]
    for date_index in range(len(swap_info)):
        date = swap_info['date'].values[date_index]
        liquidity = liquidity_info['reserve'].values[date_index]
        estimate_income = swap_info['estimate_fee'].values[date_index] / liquidity * 0.03

        price_start = swap_info['price_start'][date_index]
        price_end = swap_info['price_end'][date_index]
        estimate_il = price_start + price_end - 2 * NP.sqrt(price_start * price_end)

        estimate_mining = 10000 * 0.6 / liquidity

        single_values = [date, estimate_income, estimate_il, estimate_mining]

        for value_index in range(len(single_values)):
            total_values[value_index].append(single_values[value_index])

    temp_result = {}
    for column_index in range(len(output_columns)):
        temp_result.update({
            output_columns[column_index]: total_values[column_index]
        })
    final_result = PD.DataFrame(temp_result)
    final_result.to_csv(CT.STORE_PATH + 'pnl.csv')
    return swap_info, liquidity_info, final_result

def generate_price(start, end):
    '''
    generate price info for BTC, AVAX, DEG
    :param start: which date to start, in str, yyyy-mm-dd
    :param end: which date to end, in str
    :return:
    '''
    # start, end = '2022-04-01', '2022-04-10'
    print('Generating price records')
    output_columns = ['btc', 'avax', 'deg']
    names_in_url = ['bitcoin', 'avalanche', 'degis']
    request_headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    total_values = [[] for _ in names_in_url]
    urls = [f'https://www.coingecko.com/en/coins/{name}/historical_data/usd?start_date={start}&end_date={end}#panel' for name in names_in_url]
    date_info = []
    for url_index in range(len(urls)):
        token_info = requests.get(urls[url_index], verify=False, headers=request_headers)
        token_data = BS(token_info.text, 'lxml')
        token_result = PD.read_html(str(token_data))[0]
        date_info = token_result['Date'].values[:: -1]
        token_price = token_result['Open'].values[:: -1]
        token_price = [float(_[1: ].replace(',', '')) for _ in token_price]

        total_values[url_index] = token_price
        time.sleep(5)

    temp_result = {'date': date_info}
    for column_index in range(len(output_columns)):
        temp_result.update({
            output_columns[column_index]: total_values[column_index]
        })
    final_result = PD.DataFrame(temp_result)
    final_result.to_csv(CT.STORE_PATH + 'price_info.csv')
    return final_result

def visualization():
    # swap_info, liquidity_info, pnl_info = generate_pnl()
    swap_info, liquidity_info, pnl_info = PD.read_csv('data/swap_info.csv'), PD.read_csv('data/liquidity_info.csv'), PD.read_csv('data/pnl.csv')
    date_info = pnl_info['date'].values
    start, end = date_info[0], date_info[-1]
    # price_info = generate_price(start, end)
    price_info = PD.read_csv('data/price_info.csv')

    grid = Grid(width=1400, height=800)
    steps_axis = list(date_info)

    print('Generating swap-liquidity overview')

    # graph 001, swap information
    print('Part1. Swap')
    market_maker_line = Line('01. Swap Info.', title_top="3%", title_pos="10%")
    market_maker_line.add('Sell Volume', steps_axis, list(swap_info['sell_volume'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="20%", legend_top='3%')
    market_maker_line.add('Sell Price', steps_axis, list(swap_info['sell_price'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="20%", legend_top='3%')
    market_maker_line.add('Buy Volume', steps_axis, list(swap_info['buy_volume'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="20%", legend_top='3%')
    market_maker_line.add('Buy Price', steps_axis, list(swap_info['buy_price'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="20%", legend_top='3%')
    grid.add(market_maker_line, grid_right='55%', grid_bottom='55%')

    # graph 002, liquidity information
    print('Part2. Liquidity')
    price_line = Line('02. Liquidity Info.', title_top="3%", title_pos="55%")
    price_line.add('Liquidity In', steps_axis, list(liquidity_info['stake_volume'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="66%", legend_top='3%')
    price_line.add('Liquidity Out', steps_axis, list(liquidity_info['unstake_volume'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="66%", legend_top='3%')
    price_line.add('Reserve', steps_axis, list(liquidity_info['reserve'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="66%", legend_top='3%')
    grid.add(price_line, grid_left='55%', grid_bottom='55%')

    # graph 003, PnL information
    print('Part3. PnL')
    hedger_line = Line('03. PnL Info.', title_top="51%", title_pos="10%")
    hedger_line.add('Est. Fee', steps_axis, list(pnl_info['estimate_income'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="20%", legend_top='51%')
    hedger_line.add('Est. IL', steps_axis, list(pnl_info['estimate_il'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="20%", legend_top='51%')
    hedger_line.add('Est. Mining', steps_axis, list(pnl_info['estimate_mining'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="20%", legend_top='51%')
    hedger_line.add('Est. Total', steps_axis, list(pnl_info['estimate_income'].values - pnl_info['estimate_il'].values + pnl_info['estimate_mining'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="20%", legend_top='51%')
    grid.add(hedger_line, grid_right='55%', grid_top='55%')

    # graph 004, price information
    print('Part4. Price')
    trades_line = Line('04. Price Info.', title_top="51%", title_pos="55%")
    trades_line.add('BTC', steps_axis, list(price_info['btc'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="68%", legend_top='51%')
    trades_line.add('AVAX', steps_axis, list(price_info['avax'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="68%", legend_top='51%')
    trades_line.add('DEG', steps_axis, list(price_info['deg'].values), is_smooth=False, is_datazoom_show=True, datazoom_xaxis_index=[_ for _ in range(4)], legend_pos="68%", legend_top='51%')
    grid.add(trades_line, grid_left='55%', grid_top='55%')

    grid.render(f'demo_overview.html')

if __name__ == '__main__':
    visualization()