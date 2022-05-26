package broker.logic.polling.storm;

import broker.model.TopologyResource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataPollingSpout<T extends TopologyResource> extends BaseRichSpout {

    private static final Duration POLL_DURATION = Duration.ofSeconds(10);

    private final Fields fields;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ObjectMapper objectMapper;
    private final Class<T> consumedObjectType;

    private SpoutOutputCollector collector;

    public DataPollingSpout(Fields fields, KafkaConsumer<String, String> kafkaConsumer, Class<T> consumedObjectType) {
        this.fields = fields;
        this.kafkaConsumer = kafkaConsumer;
        this.objectMapper = new ObjectMapper();
        this.consumedObjectType = consumedObjectType;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        ConsumerRecords<String, String> consumerRecords = this.kafkaConsumer.poll(POLL_DURATION);
        this.kafkaConsumer.commitAsync();

        List<Values> transformedRecords = new ArrayList<>();
        for (ConsumerRecord<String, String> consumerRecord: consumerRecords) {
            Values transformedRecord;
            try {
                transformedRecord = this.objectMapper.readValue(consumerRecord.value(), this.consumedObjectType).toValues();
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
            transformedRecords.add(transformedRecord);
        }

        for (Values record: transformedRecords) {
            this.collector.emit(record);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(this.fields);
    }
}
