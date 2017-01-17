package in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.SYS;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionPredictor;
import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionTrain;
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
import weka.classifiers.functions.LinearRegression;
import weka.core.SerializationHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
//        String msgId = input.getStringByField("MSGID");
//        String sensorMeta=input.getStringByField("META");
//        String sensorID=input.getStringByField("SENSORID");
//        String obsType=input.getStringByField("OBSTYPE");
//        String obsVal = input.getStringByField("OBSVAL");
//        //

//
//        String msgtype = input.getStringByField("MSGTYPE");
//        String analyticsType = input.getStringByField("ANALAYTICTYPE");

//        String obsVal="22.7,49.3,0,1955.22,27"; //dummy
////        String msgId="0";
////        String sensorMeta = "meta";
//
//
//        if(msgtype.equals("modelupdate")&& analyticsType.equals("MLR")){
//            ByteArrayInputStream blobModelObject= (ByteArrayInputStream) input.getValueByField("BlobModelObject");
////            byte[] blobModelObjects = input.getBinaryByField("BlobModelObject");
//            if(l.isInfoEnabled())
//                l.info("blob model size "+blobModelObject.toString());
//
////TODO:  1- Either write model file to local disk - no task code change
////TODO:  2- Pass it as bytestream , need to update the code for task
//
//            try {
//                LinearRegressionPredictor.lr = (LinearRegression) SerializationHelper.read(blobModelObject);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            if(this.l.isInfoEnabled()) {
//                this.l.info("Model is {} ", LinearRegressionPredictor.lr.toString());
//            }
//
//        }
//
//        if(! msgtype.equals("modelupdate") ){
//            obsVal = input.getStringByField("OBSVAL");
//            msgId = input.getStringByField("MSGID");
////            sensorMeta = input.getStringByField("META");
//
//            if(l.isInfoEnabled())
//                l.info("obsVal-"+obsVal);
//        }
//        //



        HashMap<String, String> map = new HashMap();
//        obsVal="22.7,49.3,0,1955.22,27"; //dummy
        map.put(AbstractTask.DEFAULT_KEY, trainData);
        String filename=datasetName+"-MLR-"+rowkeyend+".model";
        map.put("FILENAME", filename);

        Float res = linearRegressionTrainBatched.doTask(map);
        ByteArrayOutputStream model= (ByteArrayOutputStream) linearRegressionTrainBatched.getLastResult();

        if(l.isInfoEnabled()) {
            l.info("Trained Model L.R. after bytestream object-{}", model.toString());
            l.info("res linearRegressionPredictor-" + res);
        }

        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values(model, msgId, rowkeyend,"MLR",filename));
            else {
                if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
                throw new RuntimeException();
            }
        }

    }

    @Override
    public void cleanup() {
//        linearRegressionTrainBatched.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MODEL","MSGID","ROWKEYEND","ANALAYTICTYPE","FILENAME"));
    }

}