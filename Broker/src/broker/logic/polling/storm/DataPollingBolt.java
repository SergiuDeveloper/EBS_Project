package broker.logic.polling.storm;

import broker.model.Publication;
import broker.model.TopologyResource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.text.ParseException;
import java.util.Map;

public class DataPollingBolt<T extends TopologyResource> extends BaseRichBolt {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String outputTopicFormat;
    private final Class<T> consumedObjectType;

    public DataPollingBolt(KafkaProducer<String, String> kafkaProducer, String outputTopicFormat, Class<T> consumedObjectType) {
        this.kafkaProducer = kafkaProducer;
        this.outputTopicFormat = outputTopicFormat;
        this.consumedObjectType = consumedObjectType;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {
        TopologyResource topologyResource;
        try {
            if (Publication.class.equals(consumedObjectType)) {
                topologyResource = Publication.fromTuple(tuple);
            } else {
                return;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        ProducerRecord<String, String> producerRecord = topologyResource.toProducerRecord(this.outputTopicFormat);
        this.kafkaProducer.send(producerRecord);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
