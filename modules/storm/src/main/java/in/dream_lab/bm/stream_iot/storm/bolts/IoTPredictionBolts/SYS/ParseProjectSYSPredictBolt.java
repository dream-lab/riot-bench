package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

import in.dream_lab.bm.stream_iot.tasks.parse.XMLParse;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class ParseProjectSYSPredictBolt extends BaseRichBolt {

    private Properties p;

    public ParseProjectSYSPredictBolt(Properties p_){
         p=p_;
    }

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    XMLParse xmlParse;

    //specific to stats - SYS
    String sensorDetails;
    String sensorID;
    String observedValArr;
//    private static String obsType[] ;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

//        obsType = new String[]{"DTC", "MLR", "AVG"};

    }

//sample from RowString field
//    msgId,timestamp,   source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//    1,1443033000,      ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26

//    Actual SYS sample data
//      0         1         2       3           4          5      6   7       8
//    timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//    2015-01-27T00:00:00.000Z,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26




    @Override
    public void execute(Tuple input) {
        String rowString = input.getStringByField("RowString");
        String msgId = input.getStringByField("MSGID");


{

                String[] rowStringArray = rowString.split(",");
                sensorDetails= StringUtils.join(Arrays.copyOfRange(rowStringArray, 0, 4), ","); //TODO: ts to lat
                sensorID= rowStringArray[1];
                observedValArr = StringUtils.join(Arrays.copyOfRange(rowStringArray, 4, 10),",");  // temp to aq

                if(l.isInfoEnabled())
                l.info("sensorDetails : "+sensorDetails +"observedValArr"+observedValArr);

                collector.emit(new Values(sensorDetails,sensorID,observedValArr,msgId,"parse"));


            }
    }

    @Override
    public void cleanup() {
//        xmlParse.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("RowString","MSGID","Res"));
        outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsVal","MSGID","msgtype")); // obsType = {temp, humid, airq, light, dust}
    }

}