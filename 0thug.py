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
if cur_time%period_val == 0:
  start_val = cur_time
else:
  remainder = cur_time%period_val
  start_val = int(cur_time - remainder) 

chk = False
timestamp_btc = 0
timestamp_etc = 0
timestamp_eth = 0
timestamp_doge = 0
timestamp_xrp = 0
cnt = 0


while(chk == False):
  if (timestamp_btc == 0) or (timestamp_etc == 0) or (timestamp_eth == 0) or (timestamp_doge == 0) or (timestamp_xrp == 0) :
    cnt+=1
#    print("* Try : ", cnt)
    with open('/home/ec2-user/log', 'a') as f:
        f.write(f'* Trt : {cnt}\n')

    time.sleep(10)
    url_btc = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_BTC&start=' + str(start_val) + '&end=9999999999&period=' + str(period_val)
    response_btc = requests.get(url_btc)
    
    data_btc = response_btc.json()
    timestamp_btc = data_btc[0]['date']


    url_etc = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_ETC&start=' + str(start_val) + '&end=9999999999&period=' + str(period_val)
    response_etc = requests.get(url_etc)
    
    data_etc = response_etc.json()
    timestamp_etc = data_etc[0]['date']


    url_eth = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_ETH&start=' + str(start_val) + '&end=9999999999&period=' + str(period_val)
    response_eth = requests.get(url_eth)
    
    data_eth = response_eth.json()
    timestamp_eth = data_eth[0]['date']


    url_doge = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_DOGE&start=' + str(start_val) + '&end=9999999999&period=' + str(period_val)
    response_doge = requests.get(url_doge)
    
    data_doge = response_doge.json()
    timestamp_doge = data_doge[0]['date']


    url_xrp = 'https://poloniex.com/public?command=returnChartData&currencyPair=USDT_XRP&start=' + str(start_val) + '&end=9999999999&period=' + str(period_val)
    response_xrp = requests.get(url_xrp)
    
    data_xrp = response_xrp.json()
    timestamp_xrp = data_xrp[0]['date']


  
  else :
    chk = True
  

high_btc = data_btc[0]['high']
low_btc = data_btc[0]['low']
open_btc = data_btc[0]['open']
close_btc = data_btc[0]['close']
volume_btc = data_btc[0]['volume']
datim_btc = datetime.fromtimestamp(data_btc[0]['date'])
date_btc = str(datim_btc)

data_btc={"schema":{"type":"struct","fields":[{"type":"int32","optional":True,"field":"timestamp"},{"type":"float","optional":True,"field":"high"},{"type":"float","optional":True,"field":"low"},{"type":"float","optional":True,"field":"open"},{"type":"float","optional":True,"field":"close"},{"type":"float","optional":True,"field":"volume"},{"type":"string","optional":True,"field":"date"}],"optional":False,"name":"coin"},"payload":{"timestamp":timestamp_btc,"high":high_btc,"low":low_btc,"open":open_btc,"close":close_btc,"volume":volume_btc,"date":date_btc}}


high_etc = data_etc[0]['high']
low_etc = data_etc[0]['low']
open_etc = data_etc[0]['open']
close_etc = data_etc[0]['close']
volume_etc = data_etc[0]['volume']
datim_etc = datetime.fromtimestamp(data_etc[0]['date'])
date_etc = str(datim_etc)

data_etc={"schema":{"type":"struct","fields":[{"type":"int32","optional":True,"field":"timestamp"},{"type":"float","optional":True,"field":"high"},{"type":"float","optional":True,"field":"low"},{"type":"float","optional":True,"field":"open"},{"type":"float","optional":True,"field":"close"},{"type":"float","optional":True,"field":"volume"},{"type":"string","optional":True,"field":"date"}],"optional":False,"name":"coin"},"payload":{"timestamp":timestamp_etc,"high":high_etc,"low":low_etc,"open":open_etc,"close":close_etc,"volume":volume_etc,"date":date_etc}}

high_eth = data_eth[0]['high']
low_eth = data_eth[0]['low']
open_eth = data_eth[0]['open']
close_eth = data_eth[0]['close']
volume_eth = data_eth[0]['volume']
datim_eth = datetime.fromtimestamp(data_eth[0]['date'])
date_eth = str(datim_eth)

data_eth={"schema":{"type":"struct","fields":[{"type":"int32","optional":True,"field":"timestamp"},{"type":"float","optional":True,"field":"high"},{"type":"float","optional":True,"field":"low"},{"type":"float","optional":True,"field":"open"},{"type":"float","optional":True,"field":"close"},{"type":"float","optional":True,"field":"volume"},{"type":"string","optional":True,"field":"date"}],"optional":False,"name":"coin"},"payload":{"timestamp":timestamp_eth,"high":high_eth,"low":low_eth,"open":open_eth,"close":close_eth,"volume":volume_eth,"date":date_eth}}

high_doge = data_doge[0]['high']
low_doge = data_doge[0]['low']
open_doge = data_doge[0]['open']
close_doge = data_doge[0]['close']
volume_doge = data_doge[0]['volume']
datim_doge = datetime.fromtimestamp(data_doge[0]['date'])
date_doge = str(datim_doge)

data_doge={"schema":{"type":"struct","fields":[{"type":"int32","optional":True,"field":"timestamp"},{"type":"float","optional":True,"field":"high"},{"type":"float","optional":True,"field":"low"},{"type":"float","optional":True,"field":"open"},{"type":"float","optional":True,"field":"close"},{"type":"float","optional":True,"field":"volume"},{"type":"string","optional":True,"field":"date"}],"optional":False,"name":"coin"},"payload":{"timestamp":timestamp_doge,"high":high_doge,"low":low_doge,"open":open_doge,"close":close_doge,"volume":volume_doge,"date":date_doge}}

high_xrp = data_xrp[0]['high']
low_xrp = data_xrp[0]['low']
open_xrp= data_xrp[0]['open']
close_xrp = data_xrp[0]['close']
volume_xrp = data_xrp[0]['volume']
datim_xrp = datetime.fromtimestamp(data_xrp[0]['date'])
date_xrp = str(datim_xrp)

data_xrp={"schema":{"type":"struct","fields":[{"type":"int32","optional":True,"field":"timestamp"},{"type":"float","optional":True,"field":"high"},{"type":"float","optional":True,"field":"low"},{"type":"float","optional":True,"field":"open"},{"type":"float","optional":True,"field":"close"},{"type":"float","optional":True,"field":"volume"},{"type":"string","optional":True,"field":"date"}],"optional":False,"name":"coin"},"payload":{"timestamp":timestamp_xrp,"high":high_xrp,"low":low_xrp,"open":open_xrp,"close":close_xrp,"volume":volume_xrp,"date":date_xrp}}





pro1.send('btc_coin', value = data_btc)
pro1.flush()
print('BTC Finish')

print(timestamp_btc, high_btc, low_btc, open_btc, close_btc, volume_btc, date_btc)

print('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')

pro2.send('etc_coin', value = data_etc)
pro2.flush()
print('ETC Finish')

print(timestamp_etc, high_etc, low_etc, open_etc, close_etc, volume_etc, date_etc)

print('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')

pro3.send('eth_coin', value = data_eth)
pro3.flush()
print('ETH Finish')

print(timestamp_eth, high_eth, low_eth, open_eth, close_eth, volume_eth, date_eth)

print('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')

pro4.send('doge_coin', value = data_doge)
pro4.flush()
print('DOGE Finish')

print(timestamp_doge, high_doge, low_doge, open_doge, close_doge, volume_doge, date_doge)


print('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')

pro5.send('xrp_coin', value = data_xrp)
pro5.flush()
print('XRP Finish')

print(timestamp_xrp, high_xrp, low_xrp, open_xrp, close_xrp, volume_xrp, date_xrp)

log_file.close()
