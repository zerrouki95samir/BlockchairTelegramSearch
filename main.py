import pandas as pd
import requests
from datetime import datetime, timedelta
from time import sleep
import telegram_searcher as searcher
from os import path
import math

BLOCKCHAIR_FILE_PATH = './data/blockchair.xlsx'
TELEGRAM_GROUPS__FILE_PATH = './data/telegram_groups.xlsx'

def process_data_result():

    if path.exists(BLOCKCHAIR_FILE_PATH):
        blockchair_df = pd.read_excel(BLOCKCHAIR_FILE_PATH)
    else:
        print(f'{BLOCKCHAIR_FILE_PATH} Not found!')
        return False

    if path.exists(TELEGRAM_GROUPS__FILE_PATH):
        telegram_group_df = pd.read_excel(TELEGRAM_GROUPS__FILE_PATH)
    else:
        print(f'{TELEGRAM_GROUPS__FILE_PATH} Not found!')
        return False
    
    # Drop all rows from blockchair_df if is already exist telegram_group_df
    blockchair_df = blockchair_df.loc[~(blockchair_df['id'].isin(telegram_group_df['blockchair_id']))].reset_index(drop=True)
    
    # Save blockchair_df as .xlsx file
    blockchair_df.to_excel(BLOCKCHAIR_FILE_PATH, index=False)
    
    return True


def check_if_stop(until, stop_date):
    until = datetime.strptime(until, '%Y-%m-%d %H:%M:%S')
    print(abs(stop_date - until))
    return stop_date > until

# def fill_missing(row):
#     if row['name'].strip() == '' and row['symbol'] == '':
#         return f"{row['address'][:10].lower()}..."
#     return row['name']

def blockchair_api(tf_days=1):
    print('\n------------------------------------')
    print('Execute Blockchair API Function...')
    print('------------------------------------')
    headers = {
        'authority': 'api.blockchair.com',
        'method': 'GET',
        'scheme': 'https',
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7',
        'origin': 'https://blockchair.com',
        'referer': 'https://blockchair.com/',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36'
    
    }
    sleep(2)
    sl_time = 1.5
    print(f'Fetching {tf_days}-D timeframe from blockchair API ...')
    blockchair_df = pd.DataFrame()
    limit = 100
    offset = 0
    count = 0
    r = requests.get(f'https://api.blockchair.com/ethereum/erc-20/tokens?s=time(desc)&limit={limit}')
    stop_date = r.headers['Date'].replace(' GMT', '')
    stop_date = datetime.strptime(stop_date, '%a, %d %b %Y %H:%M:%S')
    stop_date = stop_date - timedelta(days=tf_days, hours=1)
    start_url = f'https://api.blockchair.com/ethereum/erc-20/tokens?s=time(desc)&limit={limit}'
    start_url_executed = False
    block_id = 000000
    while True and count < 5: 
        url = f'https://api.blockchair.com/ethereum/erc-20/tokens?q=creating_block_id(..{block_id})&limit={limit}&s=time(desc)&offset={offset}'
        headers['path'] = f'/ethereum/erc-20/tokens?q=creating_block_id(..11784721)&s=time(desc)&limit=20&offset={offset}',
        if not start_url_executed:
            url = start_url
        r = requests.get(url)
        offset += limit
        if r.status_code == 200:
            r_json = r.json()
            if not start_url_executed:
                block_id = r_json['context'].get('state_layer_2', 11784878)
                start_url_executed = True
            blockchair_df = blockchair_df.append(
                pd.DataFrame.from_dict(r_json['data']),
                ignore_index=True
            )

            until_time = pd.DataFrame.from_dict(
                r_json['data']).sort_values(by=['time'])['time']
            
            if until_time is not None and len(until_time) > 0:
                until = until_time.values[-1]
                since = until_time.values[0]

            print(f'From: {since} until: {until} ------ ID : {block_id}')
            sleep(sl_time)
            if until:
                if check_if_stop(until, stop_date):
                    print('We reached to your timeframe sucessfully...')
                    break
            else:
                count += 1

        else:
            count += 1
            print(f'Something went wrong ({r.status_code}) on {url}')
    
    blockchair_df=blockchair_df[~(blockchair_df['time']<=str(stop_date))]
    blockchair_df= blockchair_df[~(blockchair_df['symbol'] == 'UNI-V2')]
    # Fill some missing values in name
    # blockchair_df['name'] = blockchair_df[['address', 'name', 'symbol']].apply(fill_missing, axis=1)
    if blockchair_df.empty:
        print('Empty result from blockchair api...!!!')
        return None

    blockchair_new = blockchair_df.drop(['creating_transaction_hash'], axis=1)
    blockchair_new = blockchair_new.loc[(
        blockchair_new['name'] != '') & (blockchair_new['symbol'] != '')]
    blockchair_new = blockchair_new.reset_index(drop=True)

    # Open our previous excel file:
    if path.exists(BLOCKCHAIR_FILE_PATH):
        blockchair_old = pd.read_excel(BLOCKCHAIR_FILE_PATH)
    else:
        blockchair_old = pd.DataFrame()

    # Set all rows in the new df as 0: not found
    # blockchair_new['name_found'] = 0

    # merge both old and new blockchair data...
    blockchair_df = blockchair_old.append(blockchair_new, ignore_index=True)

    # drop duplicates on block id:
    blockchair_df.drop_duplicates(subset=['id'], keep='first', inplace=True)

    # sort the new data by time.
    blockchair_df = blockchair_df.sort_values(
        by=['time'], ascending=False).reset_index(drop=True)

    # Save blockchair_df as .xlsx file
    blockchair_df.to_excel(BLOCKCHAIR_FILE_PATH, index=False)

    return blockchair_df


def telegram_search(blockchair_df):
    print('\n------------------------------------')
    print('Execute telegram seach...')
    print('------------------------------------')
    sleep(2)
    drop_dups_df = blockchair_df.drop_duplicates(subset=['name', 'symbol'], keep='first')

    # Open our previous excel file:
    if path.exists(TELEGRAM_GROUPS__FILE_PATH):
        telegram_groups_old = pd.read_excel(TELEGRAM_GROUPS__FILE_PATH)
    else:
        telegram_groups_old = pd.DataFrame(columns=[
            'blockchair_id',
            'blackchair_name',
            'blackchair_symbol',
            'telegram_username',
            'telegram_title',
            'telegram_link',
            #'telegram_subs_count',
            'telegram_channel_id',
            #'telegram_updated_at',
            'telegram_search_q'
            f'subs_count_{str(datetime.today().date())}',
        ])

    telegram_groups_new = pd.DataFrame()
    for index, row in drop_dups_df.iterrows():
        if f'{row["name"]} {row["symbol"]}' not in (telegram_groups_old['blackchair_name'] + ' ' + telegram_groups_old['blackchair_symbol']).to_list():
            # First search by name
            result = searcher.search_on(row['name'])
            search_q = row['name']

            # if we got empty values, search by symbol
            if result['username'].strip() == '':
                result = searcher.search_on(row['symbol'])
                search_q = row['symbol']

            # split name and try again. 
            if result['username'].strip() == '' and ' ' in row['name']:
                last_try = ' '.join(row['name'].strip().split(' ')[:-1])
                result = searcher.search_on(last_try)
                search_q = last_try

            if result['username'].strip() != '':
                print(f'{row["name"]} - Sucess.')
                sleep(0.5)
                telegram_groups_new = telegram_groups_new.append({
                    'blockchair_id': row['id'],
                    'blackchair_name': row['name'],
                    'blackchair_symbol': row['symbol'],
                    'telegram_username': result["username"],
                    'telegram_title': result["username"],
                    'telegram_link': f'?p=@{result["username"]}',
                    #'telegram_subs_count': result['subs_count'],
                    'telegram_channel_id': result['channel_id'],
                    'telegram_search_q': search_q,
                    f'subs_count_{str(datetime.today().date())}': result['subs_count'],
                }, ignore_index=True)

            else:
                print(f"{row['name']} - Not Found!")

        else:
            print(f'{row["name"]} Already exist in telegram_groups.xlsx file.')

    print(f'We got {len(telegram_groups_new)} new groups.')

    telegram_groups_new = telegram_groups_new.append(telegram_groups_old, ignore_index=True)

    telegram_groups_new.to_excel(TELEGRAM_GROUPS__FILE_PATH, index=False)

    print('Telegram Search Function Done successfully.')
    return len(telegram_groups_new)

# Function to update telegram_groups subs counts
def update_groups(force=False):
    print('\n------------------------------------')
    print('Execute Update Groups Function...')
    print('------------------------------------')
    sleep(2)

    def check_and_updated(row, cols):
        if not force:
            if f'subs_count_{str(datetime.today().date())}' in cols and not math.isnan(float(row[f'subs_count_{str(datetime.today().date())}'])):
                print(f'{row["blackchair_name"]} is up to date.')
                return row[f'subs_count_{str(datetime.today().date())}']

        results = searcher.search_on(row['telegram_search_q'], update=True)
        if type(results) is list:
            for r in results:
                if row['telegram_channel_id'] == r['channel_id']:
                    print(f'{row["telegram_title"]} Is Updated Successfully.')
                    return r['subs_count']

        print(f'{row["telegram_title"]} - Not Found!.')
        # Get latest record/column 
        for col in cols[::-1]:
            if 'subs_count_' in col:
                sleep(0.5)
                return row[col]
        
        sleep(0.5)
        return ''

    # Open our previous excel file:
    if path.exists(TELEGRAM_GROUPS__FILE_PATH):
        telegram_groups = pd.read_excel(TELEGRAM_GROUPS__FILE_PATH)
    else:
        print(f'{TELEGRAM_GROUPS__FILE_PATH} - Not exist!!')
        return False

    #telegram_groups[['subs_count', 'telegram_updated_at']] = telegram_groups.apply(check_and_updated, axis=1, result_type='expand')

    # Create a column with the updated date
    telegram_groups[f'subs_count_{str(datetime.today().date())}'] = telegram_groups.apply(
                                                                        lambda x: check_and_updated(
                                                                            x, telegram_groups.columns.tolist()
                                                                        ), axis=1)

    # overwrite the existing file
    telegram_groups.to_excel(TELEGRAM_GROUPS__FILE_PATH, index=False)



if __name__ == '__main__':
    begin_time = datetime.now()
    # Get latest "tf_days" blockchair data 
    blockchair_df = blockchair_api(tf_days=1)  # timeframe
    
    # blockchair_df: container both old and new blockchair data 
    # If blockchair name is already exist in telegram file then do nothing.
    # Else do seach...
    search_result_lenght = telegram_search(blockchair_df)
    
    # run full update over TELEGRAM_GROUPS__FILE_PATH. 
    # if you execute and you already have the current day values in your TELEGRAM_GROUPS__FILE_PATH nothing will happend
    # If you want to force update add param: force = True
    update_groups(force=False)
    
    # Remove rows from blockchair file if exist in telegram group
    process_data_result()

    end_time = datetime.now() - begin_time
    print('-----------------------------------------')
    print(f'\nThis script Took time : {end_time} to get {search_result_lenght} out of {len(blockchair_df)} ({round(search_result_lenght/len(blockchair_df)*100, 2)}%)')
    print('-----------------------------------------')
    