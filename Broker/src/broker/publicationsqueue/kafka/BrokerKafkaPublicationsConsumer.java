package broker.publicationsqueue.kafka;

import broker.interfaces.ISpoutDataSource;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.tuple.Values;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class BrokerKafkaPublicationsConsumer implements ISpoutDataSource {

    private static final Duration POLL_DURATION = Duration.ofSeconds(10);

    private final Consumer<String, String> consumer;
    private final Function<ConsumerRecord<String, String>, Values> consumerObjectTransformFunction;

    public BrokerKafkaPublicationsConsumer(String consumerName, String kafkaServerHost, int kafkaServerPort, Function<ConsumerRecord<String, String>, Values> consumerObjectTransformFunction) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%d", kafkaServerHost, kafkaServerPort));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerName);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(properties);
        this.consumerObjectTransformFunction = consumerObjectTransformFunction;
    }

    public void start(String publicationsQueueTopic) {
        this.consumer.subscribe(Collections.singletonList(publicationsQueueTopic));
    }

    public synchronized List<Values> pollData() {
        ConsumerRecords<String, String> consumerRecords = this.consumer.poll(POLL_DURATION);
        if (consumerRecords.count() == 0) {
            return new ArrayList<>();
        }
        this.consumer.commitAsync();

        List<Values> transformedRecords = new ArrayList<>();
        for (ConsumerRecord<String, String> consumerRecord: consumerRecords) {
            Values transformedRecord = consumerObjectTransformFunction.apply(consumerRecord);
            transformedRecords.add(transformedRecord);
        }

        return transformedRecords;
    }
}
