package broker.publicationsqueue.storm;

import broker.interfaces.IBoltDataProducer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class ProcessingBolt<T extends IBoltDataProducer> extends BaseRichBolt {

    private final T dataProducer;

    public ProcessingBolt(T dataProducer) {
        this.dataProducer = dataProducer;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {
        this.dataProducer.publishData(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
