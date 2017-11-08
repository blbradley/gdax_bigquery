import logging
import uuid
import json
from datetime import datetime

from apscheduler.schedulers.background import BlockingScheduler
import requests
from google.cloud import bigquery

import gdax


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

dataset_name = 'coinsmith'
table_name = 'gdax_polling'

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset(dataset_name)
table = dataset.table(table_name)

client_id = uuid.uuid4()

def collect():
    r = requests.get(
        'https://api.gdax.com/products/BTC-USD/ticker',
        timeout=5,
        )
    dt = datetime.utcnow()
    data = {
        'collected_at': dt.isoformat(),
        'client_id': str(client_id),
        'payload': str(r.content)
    }
    errors = bigquery_client.create_rows_json(
        table,
        [data],
        template_suffix='_ticker_' + dt.date().strftime('%Y%m%d'),
    )
    if errors:
        logging.error(errors)


if __name__ == '__main__':
    sched = BlockingScheduler()
    #sched.add_executor('processpool')
    #if gdax.enable_level2:
    #    sched.add_job(produce_level2_book, 'interval', seconds=60)
    #if gdax.enable_level3:
    #    sched.add_job(produce_level3_book, 'interval', seconds=60)
    sched.add_job(collect, 'interval', seconds=2)
    #sched.add_job(produce_trades, 'interval', seconds=2)

    try:
        sched.start()
    except KeyboardInterrupt:
        pass
