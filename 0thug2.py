import time
from datetime import datetime
import requests

from kafka import KafkaProducer
from json import dumps
import json


pro1 = KafkaProducer(
    acks=0,
    compression_type='gzip',
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
pro2 = KafkaProducer(
    acks=0,
    compression_type='gzip',
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

pro3 = KafkaProducer(
    acks=0,
    compression_type='gzip',
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

pro4 = KafkaProducer(
    acks=0,
    compression_type='gzip',
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

pro5 = KafkaProducer(
    acks=0,
    compression_type='gzip',
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

cur_time = time.time()
period_val = 300
if cur_time % period_val == 0:
  start_val = cur_time
else:
  remainder = cur_time % period_val
  start_val = int(cur_time - remainder)


btc_url = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_BTC&start=1622505600&end=9999999999&period=300'
btc_response = requests.get(btc_url)
btc_data = btc_response.json()

for i in range(len(btc_data)):
    btc_timestamp = btc_data[i]['date']
    btc_high = btc_data[i]['high']
    btc_low = btc_data[i]['low']
    btc_open = btc_data[i]['open']
    btc_close = btc_data[i]['close']
    btc_volume = btc_data[i]['volume']
    btc_datim = datetime.fromtimestamp(btc_data[i]['date'])
    btc_date = str(btc_datim)

    data_btc = {"schema": {"type": "struct", "fields": [{"type": "int32", "optional": True, "field": "timestamp"}, {"type": "float", "optional": True, "field": "high"},    {"type": "float", "optional": True, "field": "low"}, {"type": "float", "optional": True, "field": "open"}, {"type": "float", "optional": True, "field": "close"}, {
    "type": "float", "optional": True, "field": "volume"}, {"type": "string", "optional": True, "field": "date"}], "optional": False, "name": "coin"}, "payload": {"timestamp": btc_timestamp, "high": btc_high, "low": btc_low, "open": btc_open, "close": btc_close, "volume": btc_volume, "date": btc_date}}

    pro1.send('btc_coin', value=data_btc)
    pro1.flush()

print('BTC Finish')


eth_url = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_ETH&start=1622505600&end=9999999999&period=300'
eth_response = requests.get(eth_url)
eth_data = eth_response.json()

for i in range(len(eth_data)):
    eth_timestamp = eth_data[i]['date']
    eth_high = eth_data[i]['high']
    eth_low = eth_data[i]['low']
    eth_open = eth_data[i]['open']
    eth_close = eth_data[i]['close']
    eth_volume = eth_data[i]['volume']
    eth_datim = datetime.fromtimestamp(eth_data[i]['date'])
    eth_date = str(eth_datim)

    data_eth = {"schema": {"type": "struct", "fields": [{"type": "int32", "optional": True, "field": "timestamp"}, {"type": "float", "optional": True, "field": "high"},    {"type": "float", "optional": True, "field": "low"}, {"type": "float", "optional": True, "field": "open"}, {"type": "float", "optional": True, "field": "close"}, {
    "type": "float", "optional": True, "field": "volume"}, {"type": "string", "optional": True, "field": "date"}], "optional": False, "name": "coin"}, "payload": {"timestamp": eth_timestamp, "high": eth_high, "low": eth_low, "open": eth_open, "close": eth_close, "volume": eth_volume, "date": eth_date}}

    pro2.send('eth_coin', value=data_eth)
    pro2.flush()

print('ETH Finish')


etc_url = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_ETC&start=1622505600&end=9999999999&period=300'
etc_response = requests.get(etc_url)
etc_data = etc_response.json()

for i in range(len(etc_data)):
    etc_timestamp = etc_data[i]['date']
    etc_high = etc_data[i]['high']
    etc_low = etc_data[i]['low']
    etc_open = etc_data[i]['open']
    etc_close = etc_data[i]['close']
    etc_volume = etc_data[i]['volume']
    etc_datim = datetime.fromtimestamp(etc_data[i]['date'])
    etc_date = str(etc_datim)

    data_etc = {"schema": {"type": "struct", "fields": [{"type": "int32", "optional": True, "field": "timestamp"}, {"type": "float", "optional": True, "field": "high"},    {"type": "float", "optional": True, "field": "low"}, {"type": "float", "optional": True, "field": "open"}, {"type": "float", "optional": True, "field": "close"}, {
    "type": "float", "optional": True, "field": "volume"}, {"type": "string", "optional": True, "field": "date"}], "optional": False, "name": "coin"}, "payload": {"timestamp": etc_timestamp, "high": etc_high, "low": etc_low, "open": etc_open, "close": etc_close, "volume": etc_volume, "date": etc_date}}

    pro3.send('etc_coin', value=data_etc)
    pro3.flush()

print('ETC Finish')

doge_url = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_DOGE&start=1622505600&end=9999999999&period=300'
doge_response = requests.get(doge_url)
doge_data = doge_response.json()

for i in range(len(doge_data)):
    doge_timestamp = doge_data[i]['date']
    doge_high = doge_data[i]['high']
    doge_low = doge_data[i]['low']
    doge_open = doge_data[i]['open']
    doge_close = doge_data[i]['close']
    doge_volume = doge_data[i]['volume']
    doge_datim = datetime.fromtimestamp(doge_data[i]['date'])
    doge_date = str(doge_datim)

    data_doge = {"schema": {"type": "struct", "fields": [{"type": "int32", "optional": True, "field": "timestamp"}, {"type": "float", "optional": True, "field": "high"},    {"type": "float", "optional": True, "field": "low"}, {"type": "float", "optional": True, "field": "open"}, {"type": "float", "optional": True, "field": "close"}, {
    "type": "float", "optional": True, "field": "volume"}, {"type": "string", "optional": True, "field": "date"}], "optional": False, "name": "coin"}, "payload": {"timestamp": doge_timestamp, "high": doge_high, "low": doge_low, "open": doge_open, "close": doge_close, "volume": doge_volume, "date": doge_date}}

    pro4.send('doge_coin', value=data_doge)
    pro4.flush()

print('DOGE Finish')

xrp_url = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_XRP&start=1622505600&end=9999999999&period=300'
xrp_response = requests.get(xrp_url)
xrp_data = xrp_response.json()

for i in range(len(xrp_data)):
    xrp_timestamp = xrp_data[i]['date']
    xrp_high = xrp_data[i]['high']
    xrp_low = xrp_data[i]['low']
    xrp_open = xrp_data[i]['open']
    xrp_close = xrp_data[i]['close']
    xrp_volume = xrp_data[i]['volume']
    xrp_datim = datetime.fromtimestamp(xrp_data[i]['date'])
    xrp_date = str(xrp_datim)

    data_xrp = {"schema": {"type": "struct", "fields": [{"type": "int32", "optional": True, "field": "timestamp"}, {"type": "float", "optional": True, "field": "high"},    {"type": "float", "optional": True, "field": "low"}, {"type": "float", "optional": True, "field": "open"}, {"type": "float", "optional": True, "field": "close"}, {
    "type": "float", "optional": True, "field": "volume"}, {"type": "string", "optional": True, "field": "date"}], "optional": False, "name": "coin"}, "payload": {"timestamp": xrp_timestamp, "high": xrp_high, "low": xrp_low, "open": xrp_open, "close": xrp_close, "volume": xrp_volume, "date": xrp_date}}

    pro5.send('xrp_coin', value=data_xrp)
    pro5.flush()

print('XRP Finish')
