import sys
from kafka.admin import KafkaAdminClient, NewTopic


COMPANIES = ['Google', 'Microsoft', 'Facebook', 'Twitter', 'Amazon', 'Uber', 'Glovo']


if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise 'Required parameters: {kafkaServerHost} {kafkaServerPort}'

    kafka_server_host = sys.argv[1]
    kafka_server_port = int(sys.argv[2])

    admin_client = KafkaAdminClient(bootstrap_servers='{}:{}'.format(kafka_server_host, kafka_server_port), client_id='master')

    topic_list = [NewTopic(name='Publications', num_partitions=1, replication_factor=1), NewTopic(name='Subscriptions', num_partitions=1, replication_factor=1)]
    specific_publications_topic_list = [NewTopic(name='SpecificPublications-{}'.format(company), num_partitions=1, replication_factor=1) for company in COMPANIES]
    topic_list.extend(specific_publications_topic_list)
    
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except:
        pass