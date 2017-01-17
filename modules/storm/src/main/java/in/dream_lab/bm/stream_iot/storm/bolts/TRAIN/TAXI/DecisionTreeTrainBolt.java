package in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.TAXI;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeTrainBatched;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DecisionTreeTrainBolt extends BaseRichBolt {

    private Properties p;

    public DecisionTreeTrainBolt(Properties p_){
         p=p_;

    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }

    DecisionTreeTrainBatched decisionTreeTrainBatched;
    String datasetName="";

//    J48 j48tree;
//    LinearRegression lr;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));
        datasetName=p.getProperty("TRAIN.DATASET_NAME").toString();

        decisionTreeTrainBatched= new DecisionTreeTrainBatched();

        decisionTreeTrainBatched.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");
        String annotData = input.getStringByField("ANNOTDATA");
        String rowkeyend = input.getStringByField("ROWKEYEND");


//        PrintWriter out = null;
//        try {
//            out = new PrintWriter("filename-DTC-sample.txt");
//            out.println(annotData);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        out.close();


//        String obsVal="22.7,49.3,0,1955.22,27"; //dummy
//        String msgId="0";

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, annotData);
        String filename=datasetName+"-DTC-"+rowkeyend+".model";
        map.put("FILENAME", filename);

        Float res = decisionTreeTrainBatched.doTask(map);  // index of result-class/enum as return
//        ByteArrayOutputStream model= (ByteArrayOutputStream) decisionTreeTrainBatched.getLastResult();

        if(l.isInfoEnabled())
            l.info("result from res:{}",res);

        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values("model", msgId, rowkeyend,"DTC",filename));
            else {
                if (l.isWarnEnabled()) l.warn("Error in DecisionTreeClassifyBolt");
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void cleanup() {
        decisionTreeTrainBatched.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MODEL","MSGID","ROWKEYEND","ANALAYTICTYPE","FILENAME"));
    }

}