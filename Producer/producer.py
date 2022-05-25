import sys
import random
import json
from datetime import date, timedelta
from kafka import KafkaProducer


def generate_publications(companies, dates, value_min, value_max, drop_min, drop_max, variation_min, variation_max):
    while True:
        company = random.choice(companies)
        date = random.choice(dates)
        value = random.uniform(value_min, value_max)
        drop = random.uniform(drop_min, drop_max)
        variation = random.uniform(variation_min, variation_max)

        publication = (
            ('Company', company),
            ('Date', date),
            ('Value', value),
            ('Drop', drop),
            ('Variation', variation)
        )
        yield publication

def generate_dates_between(start_date, end_date):
    delta = end_date - start_date

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        yield str(day)


PUBLICATIONS_TOPIC_FORMAT = 'Publications'

COMPANIES = ['Google', 'Microsoft', 'Facebook', 'Twitter', 'Amazon', 'Uber', 'Glovo'],
DATES = list(generate_dates_between(date(2010, 4, 12), date(2018, 1, 1))),
VALUE_MIN, VALUE_MAX = -30., 30.,
DROP_MIN, DROP_MAX = -10., 10.,
VARIATION_MIN, VARIATION_MAX = 0.1, 0.8


if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise 'Required parameters: {kafkaPublicationsServerHost} {kafkaPublicationsServerPort}'

    kafka_publications_server_host = sys.argv[1]
    kafka_publications_server_port = int(sys.argv[2])
    producer = KafkaProducer(bootstrap_servers='{}:{}'.format(kafka_publications_server_host, kafka_publications_server_port))
    
    for publication in generate_publications(COMPANIES, DATES, VALUE_MIN, VALUE_MAX, DORP_MIN, DROP_MAX, VARIATION_MIN, VARIATION_MAX):
        producer.send(PUBLICATIONS_TOPIC_FORMAT, json.dumps(publication).encode())
