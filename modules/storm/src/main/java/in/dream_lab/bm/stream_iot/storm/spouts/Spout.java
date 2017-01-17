package in.dream_lab.bm.stream_iot.storm.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class Spout extends BaseRichSpout {
    SpoutOutputCollector _collector;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector= spoutOutputCollector;
    }

    public void nextTuple() {
        // TODO Read packet and forward to next bolt
        _collector.emit(new Values("Spout"));
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("packet"));
    }
}