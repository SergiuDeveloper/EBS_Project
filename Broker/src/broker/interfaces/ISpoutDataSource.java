package broker.interfaces;

import org.apache.storm.tuple.Values;

import java.util.List;

public interface ISpoutDataSource {

    List<Values> pollData();
}
