import sys
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description="")
parser.add_argument('csv', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
parser.add_argument("--rule", dest='rule', type=str, default='1T')
parser.add_argument("--index_col", dest='index_col', type=str, default='exec_date')
args = parser.parse_args()

df = pd.read_csv(args.csv, index_col=args.index_col, parse_dates=True)
df.sort_index(inplace=True)

ohlc = df['price'].resample(args.rule).ohlc()
volume = df['size'].resample(args.rule).sum()

buy_count = (df['side']=='BUY') * 1
sell_count = (df['side']=='SELL') * 1
imbalance = buy_count - sell_count
vwap = df['price']*df['size']

buy_count = buy_count.resample(args.rule).sum()
sell_count = sell_count.resample(args.rule).sum()
trades = buy_count + sell_count
imbalance = imbalance.resample(args.rule).sum()
vwap = vwap.resample(args.rule).sum()/volume

variance = df['price'].resample(args.rule).var()
stdev = df['price'].resample(args.rule).std()
average = df['price'].resample(args.rule).mean()

sell = df[df['side']=='SELL']
sell_volume = sell['size'].resample(args.rule).sum()
buy = df[df['side']=='BUY']
buy_volume = buy['size'].resample(args.rule).sum()
volume_imbalance = buy_volume-sell_volume

if args.index_col != 'exec_date':
	exec_date = pd.to_datetime(df['exec_date'])
	delay = (df.index - exec_date).dt.total_seconds()
	delay = delay.resample(args.rule).last()
else:
	delay = trades*0

sell_accepted_at = pd.to_datetime(df['sell_child_order_acceptance_id'].str[3:], format='%Y%m%d-%H%M%S-%f')
buy_accepted_at = pd.to_datetime(df['buy_child_order_acceptance_id'].str[3:], format='%Y%m%d-%H%M%S-%f')
sell_delay = (df.index - sell_accepted_at).dt.total_seconds()
buy_delay = (df.index - buy_accepted_at).dt.total_seconds()
market_delay = (df['side']=='SELL')*sell_delay + (df['side']=='BUY')*buy_delay
market_delay = market_delay.resample(args.rule).median()

data_ohlc = pd.DataFrame({'open': ohlc.open, 'high': ohlc.high, 'low': ohlc.low, 'close': ohlc.close, 'volume':volume,
	'variance':variance, 'stdev':stdev, 'average':average, 'vwap':vwap,
	'buy_count':buy_count, 'sell_count':sell_count, 'trades':trades, 'imbalance':imbalance,
	'buy_volume':buy_volume, 'sell_volume':sell_volume,'volume_imbalance':volume_imbalance,
	'delay':delay, 'market_delay':market_delay})
data_ohlc = data_ohlc.dropna()
print(data_ohlc.to_csv())
