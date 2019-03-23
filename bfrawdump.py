# -*- coding: utf-8 -*-
import aiohttp
import asyncio
import json
from datetime import datetime

KEY_CONV_TBL = {
    'channel':'c',
    'message':'m',
    'price':'p',
    'size':'z',
    'side':'d',
    'buy_child_order_acceptance_id':'i',
    'sell_child_order_acceptance_id':'I',
    'exec_date':'x',
    'mid_price':'M',
    'bids':'B',
    'asks':'A',
    "product_code": "P",
    "timestamp": "t",
    "tick_id": 'k',
    "best_bid": 'b',
    "best_ask": 'a',
    "best_bid_size": 'bz',
    "best_ask_size": 'az',
    "total_bid_depth": 'bd',
    "total_ask_depth": 'ad',
    "ltp": 'l',
    "volume": 'v',
    "volume_by_product": 'V',
}

def lightning_key_convert(dic):
    newdict = {}
    for k, v in dic.items():
        if k in KEY_CONV_TBL:
            k = KEY_CONV_TBL[k]
        # newdict[k] = v
        if type(v) is dict:
            newdict[k] = lightning_key_convert(v)
        elif type(v) is list:
            newlist = []
            for vv in v:
                newlist.append(lightning_key_convert(vv))
            newdict[k] = newlist
        else:
            newdict[k] = v
    return newdict

async def main(product_id, topics):
    while True:
        try:
            async with aiohttp.ClientSession() as client:
                ws = await client.ws_connect('wss://ws.lightstream.bitflyer.com/json-rpc')
                for t in topics:
                    await ws.send_json({'method': 'subscribe', 'params': {'channel': 'lightning_'+t+'_'+product_id}})
                while True:
                    msg = await ws.receive()
                    now = str(datetime.utcnow())
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        r = lightning_key_convert(json.loads(msg.data)['params'])
                        r['t'] = now
                        logger.info(json.dumps(r))
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        await asyncio.sleep(5)
        except Exception as e:
            print(e)
            await asyncio.sleep(1)

if __name__ == "__main__":
    import argparse
    import logging
    import logging.config

    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--product_id", dest='product_id', type=str, default='FX_BTC_JPY')
    parser.add_argument("--topics", dest='topics', type=str, nargs='*', default=['executions', 'ticker', 'board', 'board_snapshot'])
    parser.add_argument("--basename", dest='basename', type=str, default='rawdump.json')
    parser.add_argument("--count", dest='count', type=int, default=10)

    args = parser.parse_args()

    logger = logging.getLogger("bfrawdump")
    logger.setLevel(logging.INFO)

    handler = logging.handlers.TimedRotatingFileHandler(filename=args.basename, when='midnight', backupCount=args.count)
    logger.addHandler(handler)

    handler = logging.StreamHandler()
    logger.addHandler(handler)

    asyncio.get_event_loop().run_until_complete(main(args.product_id, args.topics))
