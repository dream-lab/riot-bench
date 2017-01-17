package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobDownloadTask;
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

public class AzureBlobDownloadTaskBolt extends BaseRichBolt {

    Properties p; String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public AzureBlobDownloadTaskBolt( Properties p_){
        this.csvFileNameOutSink = csvFileNameOutSink; p=p_;

    }
    OutputCollector collector;

    AzureBlobDownloadTask azureBlobDownloadTask;


    private static Logger l; // TODO: Ensure logger is initialized before use
    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

        azureBlobDownloadTask=new AzureBlobDownloadTask();


        initLogger(LoggerFactory.getLogger("APP"));

        azureBlobDownloadTask.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        // path for both model files
        String BlobModelPath = input.getStringByField("BlobModelPath");
        String analyticsType = input.getStringByField("ANALAYTICTYPE");

        String msgId = input.getString(input.size()-1);


//        azureBlobDownloadTask.doTask(rowString);
        HashMap<String, String> map = new HashMap();

        map.put(AbstractTask.DEFAULT_KEY, BlobModelPath);
        azureBlobDownloadTask.doTask(map);
        byte[] BlobModelObject = azureBlobDownloadTask.getLastResult();

        if(l.isInfoEnabled())
            l.info("downloded updated model file {} with size {}",BlobModelPath,BlobModelObject.length);


//FIXME: read and emit model for DTC
            collector.emit(new Values(BlobModelObject, msgId, "modelupdate", analyticsType,"meta"));

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("BlobModelObject","MSGID","MSGTYPE","ANALAYTICTYPE","META"));
    }

}