package in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.FIT;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionTrainBatched;
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

public class LinearRegressionTrainBolt extends BaseRichBolt {

    private Properties p;

    public LinearRegressionTrainBolt(Properties p_){
         p=p_;
    }

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    LinearRegressionTrainBatched linearRegressionTrainBatched;
    String datasetName="";
//    LinearRegression lr;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));
        linearRegressionTrainBatched= new LinearRegressionTrainBatched();
        datasetName=p.getProperty("TRAIN.DATASET_NAME").toString();
        linearRegressionTrainBatched.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {


//        //
        String msgId = input.getStringByField("MSGID");
        String trainData = input.getStringByField("TRAINDATA");
        String rowkeyend = input.getStringByField("ROWKEYEND");


//        PrintWriter out = null;
//        try {
//            out = new PrintWriter("filename1.txt");
//            out.println(trainData);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        out.close();


        HashMap<String, String> map = new HashMap();
//        obsVal="22.7,49.3,0,1955.22,27"; //dummy
        map.put(AbstractTask.DEFAULT_KEY, trainData);
        String filename=datasetName+"-MLR-"+rowkeyend+".model";
        map.put("FILENAME", filename);

        Float res = linearRegressionTrainBatched.doTask(map);
//        ByteArrayOutputStream model= (ByteArrayOutputStream) linearRegressionTrainBatched.getLastResult();

//        if(l.isInfoEnabled()) {
//            l.info("Trained Model L.R. after bytestream object-{}", model.toString());
////            l.info("res linearRegressionPredictor-" + res);
//        }

        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values("model", msgId, rowkeyend,"MLR",filename));
            else {
                if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
                throw new RuntimeException();
            }
        }

    }

    @Override
    public void cleanup() {
        linearRegressionTrainBatched.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MODEL","MSGID","ROWKEYEND","ANALAYTICTYPE","FILENAME"));
    }

}