package in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.SYS;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobDownloadTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobUploadFromStreamTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobUploadTask;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AzureBlobUploadFromStreamBolt extends BaseRichBolt {

    Properties p; String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public AzureBlobUploadFromStreamBolt(Properties p_){
        this.csvFileNameOutSink = csvFileNameOutSink; p=p_;

    }
    OutputCollector collector;

    AzureBlobUploadFromStreamTask azureBlobUploadFromStreamTask;


    private static Logger l; // TODO: Ensure logger is initialized before use
    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

        azureBlobUploadFromStreamTask= new AzureBlobUploadFromStreamTask();


        initLogger(LoggerFactory.getLogger("APP"));

        azureBlobUploadFromStreamTask.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        // path for both model files
        ByteArrayOutputStream model= (ByteArrayOutputStream)input.getValueByField("MODEL");
        String rowkeyend = input.getStringByField("ROWKEYEND");
        String msgId = input.getStringByField("MSGID");
        String analyticsType = input.getStringByField("ANALAYTICTYPE");

        HashMap<String, ByteArrayOutputStream> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, model );
//        map.put("FILENAME", (analyticsType+rowkeyend+".model"))

        azureBlobUploadFromStreamTask.doTask(map);
//        byte[] BlobModelObject = azureBlobUploadTask.getLastResult();

//FIXME: read and emit model for DTC
//            collector.emit(new Values(BlobModelObject, msgId, "modelupdate", analyticsType));

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("BlobModelObject","MSGID","MSGTYPE","ANLYTICSTYPE"));
    }

}