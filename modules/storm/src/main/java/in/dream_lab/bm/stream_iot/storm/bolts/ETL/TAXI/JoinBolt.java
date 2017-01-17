package in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI;


import in.dream_lab.bm.stream_iot.tasks.annotate.Annotate;
import in.dream_lab.bm.stream_iot.tasks.filter.RangeFilterCheck;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
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

public class JoinBolt extends BaseRichBolt {

    private Properties p;
    private int maxCountPossible ;
    private HashMap<Long,HashMap<String, String>> msgIdCountMap ;
    private ArrayList<String> schemaFieldOrderList ;
    private String schemaFieldOrderFilePath;
    private String  [] metaFields;
    
    public JoinBolt(Properties p_)
    {
         p=p_;
         maxCountPossible = Integer.parseInt(p_.getProperty("JOIN.MAX_COUNT_VALUE"));
         schemaFieldOrderFilePath = p_.getProperty("JOIN.SCHEMA_FILE_PATH");
         String metaField = p_.getProperty("JOIN.META_FIELD_SCHEMA");
         metaFields = metaField.split(",");
   
    }
    OutputCollector collector;
    private static Logger l; 
    public static void initLogger(Logger l_) {     l = l_; }
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));
        msgIdCountMap = new  HashMap<Long,HashMap<String, String>>();
        schemaFieldOrderList = new ArrayList<String>();
        
        /* reading the schema field order into a list to maintain values ordering after join */
    	try 
    	{
    		FileReader reader = new FileReader(schemaFieldOrderFilePath);
    		BufferedReader br = new BufferedReader(reader);
    		String [] values;
    		String line = br.readLine();
    		if(line != null)
    		{
    			values = line.split(",");
    			for(String s : values)
    				schemaFieldOrderList.add(s);
    			line = br.readLine();
    		}
		} 
    	catch (Exception e)
    	{
			e.printStackTrace();
		}
		
    }

    @Override
    public void execute(Tuple input) 
    {
    	String msgId = (String)input.getValueByField("MSGID");
    	String meta = (String)input.getValueByField("META");
    	String obsType = (String)input.getValueByField("OBSTYPE");
    	String obsVal = (String)input.getValueByField("OBSVAL");
    	HashMap map ;    	
    	/* if message id is present in hashmap, update */
    	Long msgIdLong = Long.parseLong(msgId);
   	
    	
    	if(msgIdCountMap.containsKey(msgIdLong) == true)
    	{
    		map = (HashMap)msgIdCountMap.get(msgIdLong);
    		map.put(obsType, obsVal);
       		msgIdCountMap.put(msgIdLong, map);
    		if(map.size() == maxCountPossible)
    		{
    			/* emit the msg as it has received all its field values also maintaining schema order*/
    			StringBuilder joinedValues = new StringBuilder();
    			for(String s : schemaFieldOrderList)
    			{
    				joinedValues.append((String)map.get(s)).append(","); 
    			}
    			joinedValues = joinedValues.deleteCharAt(joinedValues.length()-1);
    			msgIdCountMap.remove(msgIdLong);
    			collector.emit(new Values(msgId,meta,"joinedValue", joinedValues.toString()));
    		}
    	}
    	/*else add the msgId and create an hashmap for the incoming msg id */
    	else
    	{
    		map = new HashMap<String, String>();
    		map.put(obsType, obsVal);
    		
    		/* split the meta fields and add it to hash map 
    		 * to merge back into csv. This is done once only for a msg Id */
    		String [] metaVal = meta.split(",");
    		for(int i = 0; i< metaVal.length ; i++ )
    		{
    			map.put(metaFields[i], metaVal[i]);
    		}
    		msgIdCountMap.put(msgIdLong, map);
    		
    		if(map.size() == maxCountPossible)
    		{
    			/* emit the msg as it has received all its field values also maintaining schema order*/
    			StringBuilder joinedValues = new StringBuilder();
    			for(String s : schemaFieldOrderList)
    			{
    				joinedValues.append((String)map.get(s)).append(","); 
    			}
    			joinedValues = joinedValues.deleteCharAt(joinedValues.length()-1);
    			msgIdCountMap.remove(msgIdLong);
    			collector.emit(new Values(msgId,meta,"joinedValue", joinedValues.toString()));
    		}
    		
    		
    	}
    }

    @Override
    public void cleanup() {
   
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MSGID", "META", "OBSTYPE", "OBSVAL"));
    }
}
