package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeClassify;
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
import weka.classifiers.trees.J48;
import weka.core.SerializationHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DecisionTreeClassifyBolt extends BaseRichBolt {

    private Properties p;

    public DecisionTreeClassifyBolt(Properties p_){
         p=p_;

    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }

    DecisionTreeClassify decisionTreeClassify;


//    J48 j48tree;
//    LinearRegression lr;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));

        decisionTreeClassify=new DecisionTreeClassify();

        decisionTreeClassify.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        String msgtype = input.getStringByField("MSGTYPE");
        String analyticsType = input.getStringByField("ANALAYTICTYPE");
        String sensorMeta=input.getStringByField("META");


        String obsVal="22.7,49.3,0,1955.22,27"; //dummy
        String msgId="0";

        
        /* We are getting an model update message so we will update the model only*/
        
        if(msgtype.equals("modelupdate") && analyticsType.equals("DTC"))
        {
            byte[] BlobModelObject= (byte[]) input.getValueByField("BlobModelObject");
            InputStream bytesInputStream = new ByteArrayInputStream(BlobModelObject);
//        	ByteArrayInputStream BlobModelObject= (ByteArrayInputStream) input.getValueByField("BlobModelObject");
        	// do nothing for now
        //            byte[] blobModelObjects = input.getBinaryByField("BlobModelObject");
            //            p.setProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH",)

            //TODO:  1- Either write model file to local disk - no task code change
            //TODO:  2- Pass it as bytestream , update the code for task
            //TODO:  3- confirm, once this j48tree object will be updated dor not
            try {
                DecisionTreeClassify.j48tree = (J48) weka.core.SerializationHelper.read(bytesInputStream);
                if(l.isInfoEnabled()) l.info("Model is {}", DecisionTreeClassify.j48tree);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

//        if(msgtype.equals("modelupdate") && analyticsType.equals("DTC")){
//
//            try {
//                lr = (LinearRegression) SerializationHelper.read(BlobModelObject);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            if(this.l.isInfoEnabled()) {
//                this.l.info("Model is {} ", lr.toString());
//            }
//        }
        
        /* Here we are getting msg from senMl parse and we need to  need to predict */
        if(! msgtype.equals("modelupdate") )
        {
        	System.out.println("TestS : In DT bolt ");
        	 obsVal = input.getStringByField("OBSVAL");
             msgId = input.getStringByField("MSGID");
        }


        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, obsVal);
        Float res = decisionTreeClassify.doTask(map);  // index of result-class/enum as return
//        System.out.println("TestS: DT res " +res);
        if(res!=null ) {
            if(res!=Float.MIN_VALUE)
                collector.emit(new Values(sensorMeta,obsVal, msgId, res.toString(),"DTC"));
            else {
                if (l.isWarnEnabled()) l.warn("Error in DecisionTreeClassifyBolt");
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void cleanup() {
        decisionTreeClassify.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("META","OBSVAL","MSGID","RES","ANALAYTICTYPE"));
    }

}