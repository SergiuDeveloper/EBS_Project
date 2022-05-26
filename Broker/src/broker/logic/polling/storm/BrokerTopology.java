package broker.logic.polling.storm;

import broker.model.TopologyResource;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class BrokerTopology<T extends TopologyResource> {

    private static final String DATA_POLLING_SPOUT = "DataPollingSpout";
    private static final String DATA_POLLING_BOLT = "DataPollingBolt";

    private final String topologyId;
    private final Config topologyConfig;
    private final StormTopology topology;
    private final LocalCluster cluster;

    public BrokerTopology(String topologyId, String outputTopicFormat, Fields fields, KafkaConsumer<String, String> kafkaConsumer, KafkaProducer<String, String> kafkaProducer, Class<T> consumedObjectType) {
        this.topologyId = topologyId;

        DataPollingSpout<T> dataPollingSpout = new DataPollingSpout<>(fields, kafkaConsumer, consumedObjectType);
        DataPollingBolt<T> dataPollingBolt = new DataPollingBolt<>(kafkaProducer, outputTopicFormat, consumedObjectType);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(DATA_POLLING_SPOUT, dataPollingSpout);
        topologyBuilder.setBolt(DATA_POLLING_BOLT, dataPollingBolt).fieldsGrouping(DATA_POLLING_SPOUT, new Fields("company"));

        this.topologyConfig = new Config();
        this.topologyConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        this.topologyConfig.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1);

        this.cluster = new LocalCluster();
        this.topology = topologyBuilder.createTopology();
    }

    public void run() {
        this.cluster.submitTopology(topologyId, this.topologyConfig, this.topology);
    }
}
