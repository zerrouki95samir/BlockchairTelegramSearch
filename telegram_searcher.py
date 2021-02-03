
# Add your credentials
API_ID = ...
API_HASH = ""

from telethon import TelegramClient, functions
from asyncio import run



async def channel_info(username, api_id, api_hash):
    try: 
        async with TelegramClient('session', api_id, api_hash) as client:
            result = await client(functions.contacts.SearchRequest(
                q=username,
                limit=50
            ))
            return (result)
    except: 
        print('Something went wrong on searcher function..')
    return (None)
    


def search_on(q, update=False):
    out = run(channel_info(q, API_ID, API_HASH)).to_dict()
    result = out.get('chats', None)
    if result:
        if not update:
            return {
                'username': result[0].get('username', None),
                'subs_count': result[0].get('participants_count', None), 
                'channel_id': result[0].get('id', None),
                'title': result[0].get('title', None) 
            }
        else: 
            result_r = []
            for r in result: 
                result_r.append({
                    'username': r.get('username', None),
                    'subs_count': r.get('participants_count', None),
                    'channel_id': r.get('id', None),
                    'title': r.get('title', None)
                })
            return result_r
    else: 
        return {
            'username': '',
            'subs_count': '',
            'channel_id':'',
            'title': ''
        }