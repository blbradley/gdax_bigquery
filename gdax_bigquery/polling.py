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

def collect(product, type):
    r = requests.get(
        f'https://api.gdax.com/products/{product}/{type}',
        timeout=5,
        )
    dt = datetime.utcnow()
    data = {
        'collected_at': dt.isoformat(),
        'client_id': str(client_id),
        'payload': str(r.content)
    }
    product_no_dash = product.replace('-', '')
    errors = bigquery_client.create_rows_json(
        table,
        [data],
        template_suffix=f'_{type}_{product_no_dash}_' + dt.date().strftime('%Y%m%d'),
    )
    if errors:
        logging.error(errors)


if __name__ == '__main__':
    sched = BlockingScheduler()
    for product in gdax.products:
        if gdax.enable_level2:
            sched.add_job(
                collect,
                'interval',
                (product, 'level2',),
                name=f'collect_{product}_level2',
                seconds=60,
            )
        if gdax.enable_level3:
            sched.add_job(
                collect,
                'interval',
                (product, 'level3',),
                name=f'collect_{product}_level3',
                seconds=60,
            )
        sched.add_job(
            collect,
            'interval',
            (product, 'ticker',),
            name=f'collect_{product}_ticker',
            seconds=2,
        )
        sched.add_job(
            collect,
            'interval',
            (product, 'trades',),
            name=f'collect_{product}_trades',
            seconds=2,
        )

    try:
        sched.start()
    except KeyboardInterrupt:
        pass
