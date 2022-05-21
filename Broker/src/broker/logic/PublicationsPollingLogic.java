package broker.logic;

import broker.model.Publication;
import broker.publicationsqueue.kafka.BrokerKafkaPublicationsConsumer;
import broker.publicationsqueue.storm.BrokerTopology;
import broker.specificpublicationsqueue.kafka.BrokerKafkaSpecificPublicationsPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.ParseException;

public class PublicationsPollingLogic {

    private static final String PUBLICATIONS_TOPIC_FORMAT = "Publications";
    private static final String SPECIFIC_PUBLICATIONS_TOPIC_FORMAT = "SpecificPublications-%s";

    private static final String CONSUMER_NAME_FORMAT = "PublicationsConsumer-%s";
    private static final String PRODUCER_NAME_FORMAT = "SpecificPublicationsProducer-%s";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String consumerName;
    private final String kafkaPublicationsServerHost;
    private final int kafkaPublicationsServerPort;

    private final String producerName;
    private final String kafkaSpecificPublicationsServerHost;
    private final int kafkaSpecificPublicationsServerPort;


    private static Values consumerObjectTransformFunction(ConsumerRecord<String, String> consumerRecord) {
        String deserializedPublication = consumerRecord.value();
        Publication publication;
        try {
            publication = objectMapper.readValue(deserializedPublication, Publication.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return publication.toValues();
    }

    private static ProducerRecord<String, String> producerObjectTransformFunction(Tuple tuple) {
        Publication publication;
        try {
            publication = Publication.fromTuple(tuple);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }

        String company = publication.getCompany();
        String specificPublicationsTopic = String.format(SPECIFIC_PUBLICATIONS_TOPIC_FORMAT, company);
        String serializedPublication;
        try {
            serializedPublication = objectMapper.writeValueAsString(publication);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return new ProducerRecord<>(specificPublicationsTopic, null, serializedPublication);
    }

    public PublicationsPollingLogic(String company, String kafkaPublicationsServerHost, int kafkaPublicationsServerPort, String kafkaSpecificPublicationsServerHost, int kafkaSpecificPublicationsServerPort) {
        this.consumerName = String.format(CONSUMER_NAME_FORMAT, company);
        this.kafkaPublicationsServerHost = kafkaPublicationsServerHost;
        this.kafkaPublicationsServerPort = kafkaPublicationsServerPort;

        this.producerName = String.format(PRODUCER_NAME_FORMAT, company);
        this.kafkaSpecificPublicationsServerHost = kafkaSpecificPublicationsServerHost;
        this.kafkaSpecificPublicationsServerPort = kafkaSpecificPublicationsServerPort;
    }

    public void start() {
        BrokerKafkaPublicationsConsumer brokerKafkaPublicationsConsumer = new BrokerKafkaPublicationsConsumer(this.consumerName, this.kafkaPublicationsServerHost, this.kafkaPublicationsServerPort, PublicationsPollingLogic::consumerObjectTransformFunction);
        BrokerKafkaSpecificPublicationsPublisher brokerKafkaSpecificPublicationsPublisher = new BrokerKafkaSpecificPublicationsPublisher(this.producerName, this.kafkaSpecificPublicationsServerHost, this.kafkaSpecificPublicationsServerPort, PublicationsPollingLogic::producerObjectTransformFunction);

        Fields fields = new Fields("company", "value", "drop", "variation", "date");
        BrokerTopology<BrokerKafkaPublicationsConsumer, BrokerKafkaSpecificPublicationsPublisher> brokerTopology = new BrokerTopology<>(fields, "company", brokerKafkaPublicationsConsumer, brokerKafkaSpecificPublicationsPublisher);

        brokerKafkaPublicationsConsumer.start(PUBLICATIONS_TOPIC_FORMAT);
        brokerTopology.run();
    }
}
