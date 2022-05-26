package broker.model;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.tuple.Values;

public abstract class TopologyResource {

    public abstract Values toValues();
    public abstract ProducerRecord<String, String> toProducerRecord(String outputTopicFormat);
}
