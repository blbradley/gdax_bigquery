import logging
import uuid
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

def collect(type):
    r = requests.get(
        f'https://api.gdax.com/products/BTC-USD/{type}',
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
        template_suffix=f'_{type}_' + dt.date().strftime('%Y%m%d'),
    )
    if errors:
        logging.error(errors)


if __name__ == '__main__':
    sched = BlockingScheduler()
    if gdax.enable_level2:
        sched.add_job(collect, 'interval', ('level2',), name='collect_level2', seconds=60)
    if gdax.enable_level3:
        sched.add_job(collect, 'interval', ('level3',), name='collect_level3', seconds=60)
    sched.add_job(collect, 'interval', ('ticker',), name='collect_ticker', seconds=2)
    sched.add_job(collect, 'interval', ('trades',), name='collect_trades', seconds=2)

    try:
        sched.start()
    except KeyboardInterrupt:
        pass
