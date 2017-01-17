package in.dream_lab.bm.stream_iot.storm.spouts;

import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.MQTTSubscribeTask;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class MQTTSubscribeSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    static long timerWindowinMilliSec=86;

    long startTimer=0;
    long currentTime;

    MQTTSubscribeTask mqttSubscribeTask;
    BatchedFileLogging ba;

    private static Logger l; // TODO: Ensure logger is initialized before use

    public MQTTSubscribeSpout() {
    }



    public static void initLogger(Logger l_) {
        l = l_;
    }
    String spoutLogFileName=null;

    Properties p; String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public MQTTSubscribeSpout(Properties p_, String spoutLogFileName){
        this.csvFileNameOutSink = csvFileNameOutSink; p=p_;
        this.spoutLogFileName=spoutLogFileName;

    }

    public MQTTSubscribeSpout(Properties p_) {
    p=p_;
    }

    private static long msgid=1;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector= spoutOutputCollector;
        startTimer=System.currentTimeMillis();

        mqttSubscribeTask=new MQTTSubscribeTask();
        initLogger(LoggerFactory.getLogger("APP"));

        mqttSubscribeTask.setup(l,p);
        ba=new BatchedFileLogging(spoutLogFileName, topologyContext.getThisComponentId());
    }

    public void nextTuple() {
        // TODO Read packet and forward to next bolt

        long tsc=System.currentTimeMillis();



        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, "dummy");
        mqttSubscribeTask.doTask(map);
        String arg1= (String) mqttSubscribeTask.getLastResult();

//        if(l.isInfoEnabled())
//            l.info("MQTTSubscribeSpout nextTuple {}",arg1);

        //FIXME: split arg1 by colon in to BlobModelPath and analyticsType

        if(arg1!=null) {
            try {
                msgid++;
                ba.batchLogwriter(tsc,"MSGID," + msgid);
                //ba.batchLogwriter(System.nanoTime(),"MSGID," + msgId);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(l.isInfoEnabled())
                l.info("arg1 in MQTTSubscribeSpout {}",arg1);
            _collector.emit(new Values(arg1.split("-")[1], arg1, Long.toString(msgid)));
        }

    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ANALAYTICTYPE","BlobModelPath","MSGID"));
    }
}