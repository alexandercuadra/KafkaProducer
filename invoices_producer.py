from confluent_kafka import Producer
import json
import time

class InvoiceProducer:
    def __init__(self):
        self.topic = "invoices"
        #rellenar estos registros con la informaci del cluster
        self.conf = {'boostraps.servers': '',
                     'security.protocol': 'SASL_SSL',
                     'sasl.mechanism': 'PLAIN',
                     'sasl.username': '',
                     'sasl.password': '',
                     'client.id': "nombre"}

    def produce_invoices(self, producer):
        with open("data/invoices.json") as lines:
            for line in lines:
                invoice = json.loads(line)
                store_id = invoice["StoreID"]
                producer.produce(self.topic, key =  store_id)

