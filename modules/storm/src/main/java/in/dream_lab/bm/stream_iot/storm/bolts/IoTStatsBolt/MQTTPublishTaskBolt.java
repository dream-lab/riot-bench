package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;


import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.MQTTPublishTask;

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

public class MQTTPublishTaskBolt extends BaseRichBolt {

    private Properties p;

    public MQTTPublishTaskBolt(Properties p_){
         p=p_;
    }

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    MQTTPublishTask mqttpublishTask;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

        mqttpublishTask=new MQTTPublishTask();
        mqttpublishTask.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");

        String _res =  input.getStringByField("res");

        String sensorMeta=input.getStringByField("sensorMeta");
        String obsType=input.getStringByField("obsType");

        String res=sensorMeta.replaceAll(",",";")+"-"+obsType+"-"+_res;    // replacing comma in  sensorMeta by hash

//        String res="2015-01-27T06:52:23.000Z;ci527ripa000403471yii8wim;121.370579;31.196056-temp-2358.3333#2363.994#2369.6545#2375.3152#2380.9758#2386.6365#2392.2969#2397.9575#2403.6182#2409.2788#";

        if(l.isInfoEnabled())
            l.info("mqttpublishTaskres"+res);

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, res);
        mqttpublishTask.doTask(map);
        String publishedRes = (String) mqttpublishTask.getLastResult();

        if(l.isInfoEnabled())
            l.info("mqttpublishTask:"+publishedRes);



        if(publishedRes!=null ) {
                collector.emit(new Values(publishedRes,msgId));
        }
    }

    @Override
    public void cleanup() {
        mqttpublishTask.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("publishedRes","MSGID"));
    }

}