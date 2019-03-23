import sys
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description="")
parser.add_argument('csv', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
parser.add_argument("--by", dest='by', type=str, default='exec_date')
args = parser.parse_args()

df = pd.read_csv(args.csv)

df['trades'] = 1
df['buy_count'] = (df['side']=='BUY') * 1
df['sell_count'] = (df['side']=='SELL') * 1
df['imbalance'] = df['buy_count']-df['sell_count']
df['volume'] = df['size']
df['volume_imbalance'] = df['volume']*df['imbalance']
df['vwap'] = df['price']*df['volume']

if args.by in ['taker', 'maker']:
    sell = df[df['side']=='SELL']
    buy = df[df['side']=='BUY']
    df['taker'] = pd.concat([
        sell['sell_child_order_acceptance_id'],
        buy['buy_child_order_acceptance_id']])
    df['maker'] = pd.concat([
        sell['buy_child_order_acceptance_id'],
        buy['sell_child_order_acceptance_id']])

# グルーピング
agg = {c:'last' for c in df.columns}
agg['price'] = 'ohlc'
agg['volume'] = 'sum'
agg['volume_imbalance'] = 'sum'
agg['trades'] = 'sum'
agg['buy_count'] = 'sum'
agg['sell_count'] = 'sum'
agg['imbalance'] = 'sum'
agg['vwap'] = 'sum'
del agg['size']
del agg['buy_child_order_acceptance_id']
del agg['sell_child_order_acceptance_id']
if args.by in agg:
    del agg[args.by]
df_group = df.groupby(args.by).agg(agg)

# df_group['vwap'] = df_group['vwap']/df_group['volume']
#   =>なぜか計算結果がNaNになるのでvaluesを取り出して計算
df_group['vwap'] = df_group['vwap'].values/df_group['volume'].values

# カラムをフラット化
if isinstance(df_group.columns, pd.MultiIndex):
    df_group.columns = [x[1] for x in df_group.columns.ravel()]

# taker/maker グルーピング以外の場合、sideは破棄する
if args.by not in ['taker', 'maker']:
    df_group.drop(['side'], axis=1, inplace=True)

# 最後にIDでソート
df_group.sort_values('id',inplace=True)

print(df_group.to_csv())
