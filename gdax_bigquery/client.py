import logging
from datetime import datetime
import uuid
import json

import websocket
from google.cloud import bigquery

from gdax import subscription_message


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

dataset_name = 'coinsmith'
table_name = 'gdax_websocket_raw'

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset(dataset_name)
table = dataset.table(table_name)

client_id = uuid.uuid4()

def on_message(ws, message):
    dt = datetime.utcnow()
    data = {
        'collected_at': dt.isoformat(),
        'client_id': str(client_id),
        'payload': message
    }
    errors = bigquery_client.create_rows_json(
        table,
        [data],
        template_suffix='_' + dt.date().strftime('%Y%m%d'),
    )
    if errors:
        logging.error(errors)

def on_error(ws, error):
    logging.warning(error)

def on_close(ws):
    logging.warning("### closed ###")

def on_open(ws):
    msg = json.dumps(subscription_message)
    ws.send(msg)
    logging.debug('sent websocket message: {}'.format(msg))

if __name__ == "__main__":
    ws = websocket.WebSocketApp("wss://ws-feed.gdax.com",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)

    ws.on_open = on_open
    ws.run_forever()
