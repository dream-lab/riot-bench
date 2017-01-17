package in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.annotate.Annotate;
import in.dream_lab.bm.stream_iot.tasks.filter.RangeFilterCheck;

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

public class AnnotationBolt extends BaseRichBolt {

    private Properties p;

    public AnnotationBolt(Properties p_)
    {
         p=p_;

    }
    OutputCollector collector;
    private static Logger l; 
    public static void initLogger(Logger l_) {     l = l_; }
    Annotate annotateTask; 
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    	this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));
        annotateTask= new Annotate();
        annotateTask.setup(l,p);
        
        
    }

    @Override
    public void execute(Tuple input) 
    {
    	String msgId = (String)input.getValueByField("MSGID");
    	String meta = (String)input.getValueByField("META");
    	String obsType = (String)input.getValueByField("OBSTYPE");
    	String obsVal = (String)input.getValueByField("OBSVAL");
    	    	
    	HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, obsVal);
    	Float res = annotateTask.doTask(map);  
    	String updatedValue = (String) annotateTask.getLastResult();
    	collector.emit(new Values(msgId,meta,"annoatedValue" ,updatedValue));
    }

    @Override
    public void cleanup() {
    	annotateTask.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MSGID", "META", "OBSTYPE", "OBSVAL"));
    }

}
