package in.dream_lab.bm.stream_iot.storm.spouts;

import org.apache.commons.math3.util.DoubleArray;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class TimeSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    static long timerWindowinMilliSec=60000;

    long startTimer=0;
    long currentTime;


//    // for sys
//    String ROWKEYSTART="1422748800000";
//    String ROWKEYEND="1422748801000";

    // for taxi
    String ROWKEYSTART="1358102580000";
    String ROWKEYEND="1358108400000";

    static long msgid=0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector= spoutOutputCollector;
        startTimer=System.currentTimeMillis();
    }

    public void nextTuple() {
        // TODO Read packet and forward to next bolt

        currentTime=System.currentTimeMillis();

        if((currentTime-startTimer)>timerWindowinMilliSec)
        {
            msgid+=1;
            _collector.emit(new Values(Long.toString(currentTime),Long.toString(msgid),ROWKEYSTART,ROWKEYEND));
            startTimer=currentTime;

//            ROWKEYSTART=ROWKEYEND;
//            ROWKEYEND= String.valueOf(Long.parseLong(ROWKEYEND)+1000);
//            System.out.println("ROWKEYSTART:"+ROWKEYSTART+"----"+ROWKEYEND);

        }



    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("RowString","MSGID","ROWKEYSTART","ROWKEYEND"));
    }
}