package broker.logic.polling;

import broker.logic.polling.storm.BrokerTopology;
import broker.model.TopologyResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.tuple.Fields;

import java.util.Collections;
import java.util.Properties;

public class PollingLogic<T extends TopologyResource> {

    private final KafkaConsumer<String, String> generalKafkaConsumer;
    private final KafkaProducer<String, String> specificKafkaProducer;

    private final Fields fields;
    private final Class<T> consumedObjectType;

    private final String pollingTopic;
    private String outputTopicFormat = "Specific%s-%s";
    private String consumerNameFormat = "General%s-%s";
    private String producerNameFormat = "General%s-%s";

    public PollingLogic(String company, String pollingTopic, String kafkaGeneralServerHost, int kafkaGeneralServerPort, String kafkaSpecificServerHost, int kafkaSpecificServerPort, Fields fields, Class<T> consumedObjectType) {
        this.generalKafkaConsumer = this.createGeneralKafkaConsumer(company, kafkaGeneralServerHost, kafkaGeneralServerPort);
        this.specificKafkaProducer = this.createSpecificKafkaProducer(company, kafkaSpecificServerHost, kafkaSpecificServerPort);

        this.fields = fields;
        this.consumedObjectType = consumedObjectType;

        this.pollingTopic = pollingTopic;
        this.outputTopicFormat = String.format(this.outputTopicFormat, pollingTopic, "%s");
        this.consumerNameFormat = String.format(this.consumerNameFormat, pollingTopic, "%s");
        this.producerNameFormat = String.format(this.producerNameFormat, pollingTopic, "%s");
    }

    public void start() {
        BrokerTopology<T> brokerTopology = new BrokerTopology<T>(String.format("%sPollingTopology", this.pollingTopic), this.outputTopicFormat, this.fields, this.generalKafkaConsumer, this.specificKafkaProducer, this.consumedObjectType);
        brokerTopology.run();
    }

    private KafkaConsumer<String, String> createGeneralKafkaConsumer(String company, String kafkaGeneralServerHost, int kafkaGeneralServerPort) {
        String consumerName = String.format(this.consumerNameFormat, company);
        Properties properties = this.createKafkaProperties(kafkaGeneralServerHost, kafkaGeneralServerPort, consumerName);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(this.pollingTopic));
        return kafkaConsumer;
    }

    private KafkaProducer<String, String> createSpecificKafkaProducer(String company, String kafkaSpecificServerHost, int kafkaSpecificServerPort) {
        String producerName = String.format(this.producerNameFormat, company);
        Properties properties = this.createKafkaProperties(kafkaSpecificServerHost, kafkaSpecificServerPort, producerName);
        return new KafkaProducer<>(properties);
    }

    private Properties createKafkaProperties(String kafkaServerHost, int kafkaServerPort, String nodeName) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%d", kafkaServerHost, kafkaServerPort));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, nodeName);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }
}
