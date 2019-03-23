# -*- coding: utf-8 -*-
import argparse
import time
import json
from operator import itemgetter
import ccxt

parser = argparse.ArgumentParser(description="")
parser.add_argument("--pair", dest='pair', type=str, default='BTC_JPY')
parser.add_argument("--before", dest='before', action="store_true")
parser.add_argument("--id", dest='id', type=int, default=0)
parser.add_argument("--waitsec", dest='waitsec', type=float, default=1)
args = parser.parse_args()

api = ccxt.bitflyer()
api.urls['api'] = 'https://api.bitflyer.com'

api_args = {}
api_args['product_code'] = args.pair
api_args['count'] = 500

if args.before:
    direction = 'before'
else:
    direction = 'after'

if args.id > 0:
    api_args[direction] = args.id

last_id = 0
once = True
while True:
    try:
        messages = api.publicGetGetexecutions(api_args)
        if type(messages) is list:
            messages.sort(key = itemgetter('id'), reverse=args.before)
            for e in messages:
                if once:
                    print(','.join([str(x) for x in e.keys()]))
                    once = False
                print(','.join([str(x) for x in e.values()]))
                last_id = e['id']
            api_args[direction] = last_id
    except Exception as e:
        time.sleep(10)
    time.sleep((-time.time() % args.waitsec) or args.waitsec)
