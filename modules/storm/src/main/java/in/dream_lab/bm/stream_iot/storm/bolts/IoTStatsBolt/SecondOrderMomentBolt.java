package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.statistics.SecondOrderMoment;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SecondOrderMomentBolt extends BaseRichBolt {

    private Properties p;

    public SecondOrderMomentBolt(Properties p_){
         p=p_;
    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    Map<String, SecondOrderMoment> momentMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));
        momentMap = new HashMap<String, SecondOrderMoment>();

    }

//from bloom -    outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsType","obsVal","MSGID"));

    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");
        String sensorMeta=input.getStringByField("sensorMeta");
        String sensorID=input.getStringByField("sensorID");
        String obsType=input.getStringByField("obsType");
        String obsVal = input.getStringByField("obsVal");


        String key = sensorID + obsType;
        SecondOrderMoment secondOrderMoment = momentMap.get(key);
        if(secondOrderMoment == null){
            secondOrderMoment=new SecondOrderMoment();
            secondOrderMoment.setup(l,p);
            momentMap.put(key, secondOrderMoment);
        }
        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, obsVal);
        secondOrderMoment.doTask(map);

        Float res = (Float) secondOrderMoment.getLastResult();

        if(l.isInfoEnabled())
            l.info("secondordermoment:"+res);


        if(res!=null ) {
            if(res!=Float.MIN_VALUE) {

                collector.emit(new Values(sensorMeta,sensorID,obsType,res.toString(),msgId));

            }
            else {
                if (l.isWarnEnabled()) l.warn("Error in secondordermomentBolt");
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsType","res","MSGID"));
    }

}