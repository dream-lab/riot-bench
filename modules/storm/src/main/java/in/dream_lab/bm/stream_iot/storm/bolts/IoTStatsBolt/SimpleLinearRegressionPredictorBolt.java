package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.SimpleLinearRegressionPredictor;

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

public class SimpleLinearRegressionPredictorBolt extends BaseRichBolt {

    private Properties p;
    public SimpleLinearRegressionPredictorBolt(Properties p_){
         p=p_;
    }

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
//    SimpleLinearRegressionPredictor simpleLinearRegressionPredictor;

    Map<String, SimpleLinearRegressionPredictor> slrmap; //kalmanFilter;

//    outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsType","kalmanUpdatedVal","MSGID"));

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

        slrmap = new HashMap<String, SimpleLinearRegressionPredictor>();
//        simpleLinearRegressionPredictor=new SimpleLinearRegressionPredictor();
//        simpleLinearRegressionPredictor.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {
        String msgId = input.getStringByField("MSGID");
        String sensorMeta=input.getStringByField("META");

        String sensorID=input.getStringByField("SENSORID");
        String obsType=input.getStringByField("OBSTYPE");
        String key = sensorID + obsType;

        String kalmanUpdatedVal = input.getStringByField("kalmanUpdatedVal");
       
        
//        simpleLinearRegressionPredictor.doTask(kalmanUpdatedVal);

        SimpleLinearRegressionPredictor simpleLinearRegressionPredictor = slrmap.get(key);
        if(simpleLinearRegressionPredictor == null){
            simpleLinearRegressionPredictor=new SimpleLinearRegressionPredictor();
            simpleLinearRegressionPredictor.setup(l,p);
            slrmap.put(key, simpleLinearRegressionPredictor);
        }
        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, kalmanUpdatedVal);
        simpleLinearRegressionPredictor.doTask(map);
        

        float[] res= simpleLinearRegressionPredictor.getLastResult();
        if(res!=null) {
            StringBuffer resTostring = new StringBuffer();

// replacing , by # Because MQTT  uses comma for seperating

            for (int c = 0; c < res.length; c++) {
                resTostring.append(res[c]);
                resTostring.append("#");
            }

//            if(l.isInfoEnabled())
//            l.info("simpleLinearRegressionPredictorRes:" + resTostring);
//            
            sensorMeta = sensorMeta.concat(",").concat(obsType);
            obsType = "SLR";
            collector.emit(new Values(sensorID ,sensorMeta, obsType, resTostring.toString(), msgId));
        }

    }

    @Override
    public void cleanup() 
    {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("SENSORID","META","OBSTYPE","res","MSGID"));
    }

}