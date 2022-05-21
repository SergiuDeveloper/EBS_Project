package broker.publicationsqueue.storm;

import broker.interfaces.ISpoutDataSource;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class SourcePublicationsSpout<T extends ISpoutDataSource> extends BaseRichSpout {

    private final Fields fields;
    private final T dataSource;

    private SpoutOutputCollector collector;

    public SourcePublicationsSpout(Fields fields, T dataSource) {
        this.fields = fields;
        this.dataSource = dataSource;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        List<Values> records = this.dataSource.pollData();
        for (Values record: records) {
            this.collector.emit(record);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(this.fields);
    }
}
