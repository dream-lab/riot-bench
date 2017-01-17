package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI;

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

public class ParseProjectTAXIPredictBolt extends BaseRichBolt {

    private Properties p;

    public ParseProjectTAXIPredictBolt(Properties p_){
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


    }

//sample from RowString field
//    msgId,timestamp,   source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//    1,1443033000,      ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26

//Actual TAXI sample data
//    0                 1           2               3           4                   5           6                   7               8                   9           10                  11        12       13      14         15          16
//taxi_identifier,hack_license,pickup_datetime,timestamp,trip_time_in_secs,    trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type    ,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
//024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14 03:57:00,2013-01-14 04:23:00,1560,19.36,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,0.00,0.50,13.00,4.80,70.30




    @Override
    public void execute(Tuple input) {
        String rowString = input.getStringByField("RowString");
        String msgId = input.getStringByField("MSGID");


{

                String[] rowStringArray = rowString.split(",");
                sensorDetails= StringUtils.join(Arrays.copyOfRange(rowStringArray, 0, 3), ","); //TODO: ts to lat
                sensorID= rowStringArray[0];
                observedValArr = StringUtils.join(Arrays.copyOfRange(rowStringArray, 4, 17),",");  // temp to aq

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