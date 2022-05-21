package broker.publicationsqueue.storm;

import broker.interfaces.IBoltDataProducer;
import broker.interfaces.ISpoutDataSource;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class BrokerTopology<T1 extends ISpoutDataSource, T2 extends IBoltDataProducer> {

    private static final String TOPOLOGY_ID = "BrokerTopology";
    private static final String SOURCE_PUBLICATIONS_SPOUT_ID = "SourcePublicationsSpout";
    private static final String PROCESSING_BOLT_ID = "ProcessingBolt";

    private final Config topologyConfig;
    private final StormTopology topology;
    private final LocalCluster cluster;

    public BrokerTopology(Fields fields, String companyFieldName, T1 dataSource, T2 dataProducer) {
        SourcePublicationsSpout<T1> sourcePublicationsSpout = new SourcePublicationsSpout<T1>(fields, dataSource);
        ProcessingBolt<T2> processingBolt = new ProcessingBolt<T2>(dataProducer);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SOURCE_PUBLICATIONS_SPOUT_ID, sourcePublicationsSpout);
        topologyBuilder.setBolt(PROCESSING_BOLT_ID, processingBolt).fieldsGrouping(SOURCE_PUBLICATIONS_SPOUT_ID, new Fields(companyFieldName));

        this.topologyConfig = new Config();
        this.topologyConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        this.topologyConfig.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1);

        this.cluster = new LocalCluster();
        this.topology = topologyBuilder.createTopology();
    }

    public void run() {
        this.cluster.submitTopology(TOPOLOGY_ID, this.topologyConfig, this.topology);
    }
}
