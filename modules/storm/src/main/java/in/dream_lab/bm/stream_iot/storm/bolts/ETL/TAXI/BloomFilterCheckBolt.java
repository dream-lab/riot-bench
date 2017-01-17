package in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.filter.BloomFilterCheck;
import in.dream_lab.bm.stream_iot.tasks.filter.MultipleBloomFilterCheck;

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

public class BloomFilterCheckBolt  extends BaseRichBolt {

    private Properties p;

    public BloomFilterCheckBolt(Properties p_)
    {
         p=p_;

    }
    OutputCollector collector;
    private static Logger l; 
    public static void initLogger(Logger l_) {     l = l_; }
    MultipleBloomFilterCheck bloomFilterCheckTask; 
    int useMsgField ;
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));

        bloomFilterCheckTask= new MultipleBloomFilterCheck();

        bloomFilterCheckTask.setup(l,p);
        
        useMsgField = Integer.parseInt(p.getProperty("FILTER.BLOOM_FILTER_CHECK.USE_MSG_FIELD"));
    }

    @Override
    public void execute(Tuple input) 
    {
    	String msgId = (String)input.getValueByField("MSGID");
    	String meta = (String)input.getValueByField("META");
    	String obsType = (String)input.getValueByField("OBSTYPE");
    	String obsVal = (String)input.getValueByField("OBSVAL");
    	String sensorId = (String)input.getValueByField("SENSORID");
    	
    	if(useMsgField >0 )
    	{
    		HashMap<String, String> map = new HashMap();
	        map.put(obsType, obsVal);
	    	
	    	Float res = bloomFilterCheckTask.doTask(map); 
	    	String updatedValue = (res != 0) ? obsVal : "null";
	    	collector.emit(new Values(msgId, sensorId ,meta,obsType ,updatedValue));
    	}
    	else 
    	{
    		collector.emit(new Values(msgId, sensorId ,meta,obsType ,obsVal));
    	}
    }

    @Override
    public void cleanup() {
    	bloomFilterCheckTask.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("MSGID", "SENSORID" , "META", "OBSTYPE", "OBSVAL"));
    }

}

