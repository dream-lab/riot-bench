package in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.FIT;

import in.dream_lab.bm.stream_iot.tasks.io.AzureTableRangeQueryTaskFIT;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableRangeQueryTaskTAXI;
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

public class AzureTableRangeQueryBolt extends BaseRichBolt {

    Properties p; String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public AzureTableRangeQueryBolt(Properties p_){
        this.csvFileNameOutSink = csvFileNameOutSink; p=p_;

    }
    OutputCollector collector;

    AzureTableRangeQueryTaskFIT azureTableRangeQueryTaskFIT;


    private static Logger l; // TODO: Ensure logger is initialized before use
    public static void initLogger(Logger l_) {
        l = l_;
    }
    String ROWKEYSTART;
    String ROWKEYEND;


    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

        azureTableRangeQueryTaskFIT = new AzureTableRangeQueryTaskFIT();


        initLogger(LoggerFactory.getLogger("APP"));

        azureTableRangeQueryTaskFIT.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        // path for both model files
//        String BlobModelPath = input.getStringByField("BlobModelPath");
//        String analyticsType = input.getStringByField("analyticsType");

        String msgId = input.getStringByField("MSGID");
        ROWKEYSTART= (input.getStringByField("ROWKEYSTART"));
        ROWKEYEND= (input.getStringByField("ROWKEYEND"));


        if(l.isInfoEnabled())
            l.info("ROWKEYSTART:{} ROWKEYEND{}",ROWKEYSTART,ROWKEYEND);

        HashMap<String, String> map = new HashMap();
        map.put("ROWKEYSTART", ROWKEYSTART);
        map.put("ROWKEYEND", ROWKEYEND);

//        Stopwatch stopwatch=null;
//        if(l.isInfoEnabled()) {
//            stopwatch = Stopwatch.createStarted(); //
//        }

        azureTableRangeQueryTaskFIT.doTask(map);



        Iterable<AzureTableRangeQueryTaskFIT.FIT_data> result= (Iterable<AzureTableRangeQueryTaskFIT.FIT_data>) azureTableRangeQueryTaskFIT.getLastResult();

        StringBuffer bf=new StringBuffer();
        // Loop through the results, displaying information about the entity
        for (AzureTableRangeQueryTaskFIT.FIT_data entity : result) {
//            if(l.isInfoEnabled())
//            l.info("partition key {} and fareamount{}",entity.getPartitionKey(),entity.getFare_amount());

            bf  .append(entity.getAcc_ankle_x()).append(",")
                .append(entity.getAcc_ankle_y()).append(",")
                .append(entity.getAcc_ankle_z()).append(",")

                .append(entity.getAcc_arm_x()).append(",")
                .append(entity.getAcc_arm_y()).append(",")
                .append(entity.getAcc_arm_z()).append(",")

                .append(entity.getAcc_chest_x()).append(",")
                .append(entity.getAcc_chest_y()).append(",")
                .append(entity.getAcc_chest_z()).append(",")
                .append(entity.getEcg_lead_1())
                    .append("\n");


        }
//        if(l.isInfoEnabled()) {
//            stopwatch.stop(); // optional
//            l.info("Time elapsed for azureTableRangeQueryTask() is {}", stopwatch.elapsed(MILLISECONDS)); //
//        }

        if(l.isInfoEnabled())
            l.info("data for annotation {}",bf.toString());

//FIXME: read and emit model for DTC
            collector.emit(new Values(bf.toString(), msgId,ROWKEYEND ));

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("TRAINDATA","MSGID","ROWKEYEND"));
    }

}