import os
import json

enable_level2 = os.getenv('GDAX_ENABLE_LEVEL2', '1') == '1'
enable_level3 = os.getenv('GDAX_ENABLE_LEVEL3', '0') == '1'

channels = ['ticker']

if enable_level2:
  channels += ['level2']
if enable_level3:
  channels += ['full']

subscription_message = {
    'type': 'subscribe',
    'product_ids': ['BTC-USD'],
    'channels': channels,
}
