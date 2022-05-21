package broker.specificpublicationsqueue.kafka;

import broker.interfaces.IBoltDataProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.tuple.Tuple;

import java.util.Properties;
import java.util.function.Function;

public class BrokerKafkaSpecificPublicationsPublisher implements IBoltDataProducer {

    private final Producer<String, String> producer;
    private final Function<Tuple, ProducerRecord<String, String>> producerObjectTransformFunction;

    public BrokerKafkaSpecificPublicationsPublisher(String producerName, String kafkaServerHost, int kafkaServerPort, Function<Tuple, ProducerRecord<String, String>> producerObjectTransformFunction) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%d", kafkaServerHost, kafkaServerPort));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, producerName);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.producer = new KafkaProducer<>(properties);
        this.producerObjectTransformFunction = producerObjectTransformFunction;
    }

    public void publishData(Tuple tuple) {
        ProducerRecord<String, String> producerRecord = this.producerObjectTransformFunction.apply(tuple);
        this.producer.send(producerRecord);
    }
}
