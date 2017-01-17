package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ErrorEstimationBolt extends BaseRichBolt {

    private Properties p;

    private String Res="0";
    private String avgRes="0";

    public ErrorEstimationBolt(Properties p_){
         p=p_;
    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {


        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

    }

// From L.R.
//    outputFieldsDeclarer.declare(new Fields("sensorMeta","obsVal","MSGID","Res","analyticsType"));

    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");
        String analyticsType = input.getStringByField("ANALAYTICTYPE");
//        String sensorID=input.getStringByField("sensorID");
        String sensorMeta=input.getStringByField("META");
        String obsVal = input.getStringByField("OBSVAL");
        float fare_amount = Float.parseFloat((input.getStringByField("OBSVAL")).split(",")[2]);



        if(analyticsType.equals("MLR")) {
            Res  = input.getStringByField("RES");
        }


        if(analyticsType.equals("AVG")) {
            avgRes = input.getStringByField("AVGRES");

            if(l.isInfoEnabled())
            l.info("avgRes:"+avgRes);
        }


//        float air_quality= Float.parseFloat(obsVal.split(",")[4]);

        if(l.isInfoEnabled())
            l.info("analyticsType:{},Res:{},avgRes:{}",analyticsType,Res,avgRes);


        if(analyticsType.equals("MLR")) {
        float errval= (fare_amount-Float.parseFloat(Res)) /Float.parseFloat(avgRes);
        if(l.isInfoEnabled())
            l.info(("errval -"+errval));
            collector.emit(new Values(sensorMeta, errval, msgId, analyticsType, obsVal));
        }

        }



    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("META","ERROR","MSGID","ANALAYTICTYPE","OBSVAL"));
    }

}