package broker.interfaces;

import org.apache.storm.tuple.Tuple;

public interface IBoltDataProducer {

    void publishData(Tuple tuple);
}
