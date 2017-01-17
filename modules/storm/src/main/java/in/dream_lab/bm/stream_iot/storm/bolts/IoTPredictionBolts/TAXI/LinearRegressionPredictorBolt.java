package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionPredictor;
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
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LinearRegressionPredictorBolt extends BaseRichBolt {

    private Properties p;

    public LinearRegressionPredictorBolt(Properties p_){
         p=p_;
    }

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    LinearRegressionPredictor linearRegressionPredictor;
//    LinearRegression lr;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));
        linearRegressionPredictor=new LinearRegressionPredictor();
        linearRegressionPredictor.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {


//        //
//        String msgId = input.getStringByField("MSGID");
        String sensorMeta=input.getStringByField("META");
//        String sensorID=input.getStringByField("SENSORID");
//        String obsType=input.getStringByField("OBSTYPE");
//        String obsVal = input.getStringByField("OBSVAL");
//        //

//
        String msgtype = input.getStringByField("MSGTYPE");
        String analyticsType = input.getStringByField("ANALAYTICTYPE");

        String obsVal="10,1955.22,27"; //dummy
        String msgId="0";
//        String sensorMeta = "meta";


        if(msgtype.equals("modelupdate")&& analyticsType.equals("MLR")){
            byte[] BlobModelObject= (byte[]) input.getValueByField("BlobModelObject");
            InputStream bytesInputStream = new ByteArrayInputStream(BlobModelObject);
//            if(l.isInfoEnabled())
//                l.info("blob model size "+blobModelObject.toString());

//TODO:  1- Either write model file to local disk - no task code change
//TODO:  2- Pass it as bytestream , need to update the code for task

            try {
                LinearRegressionPredictor.lr = (LinearRegression) SerializationHelper.read(bytesInputStream);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(this.l.isInfoEnabled()) {
                this.l.info("Model is updated MLR {} ", LinearRegressionPredictor.lr.toString());
            }

        }

        if(! msgtype.equals("modelupdate") ){
            obsVal = input.getStringByField("OBSVAL");
            msgId = input.getStringByField("MSGID");
//            sensorMeta = input.getStringByField("META");

            if(l.isInfoEnabled())
                l.info("modelupdate obsVal-"+obsVal);
        }
        //



        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, obsVal);
        Float res = linearRegressionPredictor.doTask(map);

        if(l.isInfoEnabled())
        l.info("res linearRegressionPredictor-"+res);

        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values(sensorMeta,obsVal, msgId, res.toString(),"MLR"));
            else {
                if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
                throw new RuntimeException();
            }
        }

    }

    @Override
    public void cleanup() {
        linearRegressionPredictor.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("META","OBSVAL","MSGID","RES","ANALAYTICTYPE"));
    }

}