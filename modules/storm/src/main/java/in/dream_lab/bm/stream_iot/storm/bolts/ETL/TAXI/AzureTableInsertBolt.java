package in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.filter.RangeFilterCheck;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableBatchInsert;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableInsert;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureTableInsertBolt  extends BaseRichBolt {

    private Properties p;

    public AzureTableInsertBolt(Properties p_)
    {
         p=p_;

    }
    OutputCollector collector;
    private static Logger l; 
    public static void initLogger(Logger l_) {     l = l_; }
    AzureTableBatchInsert azureTableInsertTask; 
    private HashMap<String, String> tuplesMap ;
    private int insertBatchSize ;
    private String batchFirstMsgId;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));

        azureTableInsertTask= new AzureTableBatchInsert();

        azureTableInsertTask.setup(l,p);
        tuplesMap = new HashMap<String, String>();
        insertBatchSize = Integer.parseInt(p.getProperty("IO.AZURE_TABLE.INSERTBATCHSIZE", "100"));
    }

    @Override
    public void execute(Tuple input) 
    {
    	String msgId = (String)input.getValueByField("MSGID");
    	String meta = (String)input.getValueByField("META");
    	String obsType = (String)input.getValueByField("OBSTYPE");
    	int count = tuplesMap.size();
    	
    	if(count == 0 )
    		batchFirstMsgId = msgId;
    	tuplesMap.put(String.valueOf(count), (String)input.getValueByField("OBSVAL"));
    	if(tuplesMap.size() >= insertBatchSize )
    	{
    		Float res = azureTableInsertTask.doTask(tuplesMap);
    		tuplesMap = new HashMap<>();
    	 	//collector.emit(new Values(batchFirstMsgId, meta, obsType, (String)input.getValueByField("OBSVAL")));
    	}
    	collector.emit(new Values(msgId, meta, obsType, (String)input.getValueByField("OBSVAL")));
    	
    }

    @Override
    public void cleanup() {
    	azureTableInsertTask.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("MSGID", "META", "OBSTYPE", "OBSVAL"));
    }

}
