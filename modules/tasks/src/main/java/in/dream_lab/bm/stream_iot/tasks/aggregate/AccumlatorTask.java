package in.dream_lab.bm.stream_iot.tasks.aggregate;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.utils.TimestampValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;

import org.slf4j.Logger;

/**
 * @author shilpa
 *
 */
public class AccumlatorTask extends AbstractTask<String, Map<String, Map<String, Queue<TimestampValue>>>> {

	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static ArrayList<String> multiValueObsType;
	private static int tupleWindowSize;
	//For indication postion of timestamp field among other fields present in meta
	private static int timestampField; 
	private Map<String, Map<String, Queue<TimestampValue>>> valuesMap;
	private int counter;
	private HashMap<String, String> bmMap ;
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if (!doneSetup) {
				String multiValueObs = p_.getProperty("AGGREGATE.ACCUMLATOR.MULTIVALUE_OBSTYPE");
				String [] multiValObs =  multiValueObs.split(",");
				multiValueObsType = new ArrayList<String>();
				for(String s : multiValObs)
					multiValueObsType.add(s);
				
				tupleWindowSize = Integer.parseInt(p_.getProperty("AGGREGATE.ACCUMLATOR.TUPLE_WINDOW_SIZE")); //TODO: Move to aggregate property namespace
				timestampField = Integer.parseInt(p_.getProperty("AGGREGATE.ACCUMLATOR.META_TIMESTAMP_FIELD")); // TODO: What do we use this for?
				doneSetup = true;
			}
			valuesMap = new HashMap<String, Map<String, Queue<TimestampValue>>>();
			counter = 0;
		}
	}

	@Override
	public Float doTaskLogic(Map<String, String> map) {
		String sensorId = map.get("SENSORID");
		String metaValues = map.get("META");
		String obsVal = map.get("OBSVALUE");
		String obsType = map.get("OBSTYPE");
		
		String[] metaVal = metaValues.split(",");
		assert metaVal.length > 0;
		String meta = metaVal[metaVal.length - 1];
		
		//Used to store timestamp of obsVal  
		String ts = metaVal[timestampField];
		
		Map<String, Queue<TimestampValue>> innerHashMap;
		Queue<TimestampValue> queue = null;
		try {
			counter++;
			StringBuilder key = new StringBuilder().append(sensorId).append(obsType);
			innerHashMap = valuesMap.get(key.toString());
			if (innerHashMap == null) {
				innerHashMap = new HashMap<String, Queue<TimestampValue>>();
				queue = new PriorityQueue<TimestampValue>();
				innerHashMap.put(meta, queue);
				valuesMap.put(key.toString(), innerHashMap);
			}
			// If obsType is already present we need to append the incoming new value
			queue = innerHashMap.get(meta);
			if (queue == null) {
				queue = new PriorityQueue<TimestampValue>();
				innerHashMap.put(meta, queue);
			}
			// Considering special case of SLR values since it sends multiple values
			// separated by hash
			if (multiValueObsType.contains(obsType)) { // FIXME: Make generic by using a property for values that require split!
				String[] predValues = obsVal.split("#");
				TimestampValue tsVal;
				for (String s : predValues) {
					tsVal = new TimestampValue(s, ts);
					queue.add(tsVal);
				}
			}
			// add the values to end of queue for other single valued obsType
			else {
				TimestampValue tsVal = new TimestampValue(obsVal, ts);
				queue.add(tsVal);
			}
			innerHashMap.put(meta, queue);
			valuesMap.put(key.toString(), innerHashMap);
			// accumlate msg till it reaches threshold
			if (counter == tupleWindowSize) {
				// setlast result to outerhashmap
				setLastResult(valuesMap);
				valuesMap = new HashMap<String, Map<String, Queue<TimestampValue>>>();
				counter = 0;
				return 1.0f;
			} else
				return 0.0f;
		}
		catch(Exception e)
		{
			l.error("Exception occured in Accumlator do task :  "+ e.getMessage());
			return -1.0f;
		}
	}
}