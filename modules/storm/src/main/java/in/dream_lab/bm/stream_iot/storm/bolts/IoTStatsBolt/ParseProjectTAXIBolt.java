package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;

import in.dream_lab.bm.stream_iot.tasks.parse.XMLParse;
import org.apache.storm.shade.org.apache.commons.lang.ArrayUtils;
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

public class ParseProjectTAXIBolt extends BaseRichBolt {

    private Properties p;

    public ParseProjectTAXIBolt(Properties p_){
         p=p_;
    }

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    XMLParse xmlParse;

    //specific to stats - SYS
    String taxiDetails;
    String taxiID;
    String[] observedValArr;
    String[] observedValArrtemp;
    private static String obsType[] ;
    //

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

//        obsType = new String[]{"temp", "humid", "airq", "light", "dust"};
        obsType = new String[]{"trip_time_in_secs","trip_distance","fare_amount","surcharge","mta_tax","tip_amount","tolls_amount","total_amount"}; // 4,5, 11 to end

    }


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

                taxiDetails= StringUtils.join(Arrays.copyOfRange(rowStringArray, 0, 4), ","); //TODO: up to 3 rd
                taxiID= rowStringArray[0];

                observedValArrtemp= new String[]{rowStringArray[4], rowStringArray[5]};
                observedValArr = (String[])ArrayUtils.addAll(observedValArrtemp, Arrays.copyOfRange(rowStringArray, 11, 17));

                if(l.isInfoEnabled())
                	l.info("taxiDetails : "+taxiDetails +"observedValArr TAXI -"+Arrays.toString(observedValArr));


                for(int obsIndex=0;obsIndex<observedValArr.length;obsIndex++)
                {
//                    System.out.println("Emitting form parse bolt " +taxiDetails + " "+taxiID + " "+obsType[obsIndex] + " "+observedValArr[obsIndex] + " "+msgId);
                	collector.emit(new Values(taxiDetails,taxiID,obsType[obsIndex],observedValArr[obsIndex],msgId));
                }

            }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sensorMeta","sensorID","obsType","obsVal","MSGID")); // obsType = {temp, humid, airq, light, dust}
    }

}