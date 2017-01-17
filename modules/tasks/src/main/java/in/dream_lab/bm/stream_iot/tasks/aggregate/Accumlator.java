package in.dream_lab.bm.stream_iot.tasks.aggregate;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.utils.TimestampValue;

import java.util.Properties;
import java.util.Queue;

import org.slf4j.Logger;

public class Accumlator extends AbstractTask<String, Map<String, Map<String, Queue<TimestampValue>>>> {

	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;

	private Map<String, Map<String, Queue<TimestampValue>>> valuesMap;
	private int tupleWindowSize;
	private int counter;

	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if (!doneSetup) {
				doneSetup = true;
			}
			valuesMap = new HashMap<String, Map<String, Queue<TimestampValue>>>();
			tupleWindowSize = Integer.parseInt(p_.getProperty("VISUALIZE.TUPLE_WINDOW_SIZE")); //TODO: Move to aggregate property namespace
			counter = 0;
		}
	}

	@Override
	public Float doTaskLogic(Map<String, String> map) {
		String sensorId = map.get("SENSORID");
		String meta = map.get("META");
		String obsVal = map.get("OBSVALUE");
		String obsType = map.get("OBSTYPE");
		String ts = map.get("TS");

		Map<String, Queue<TimestampValue>> innerHashMap;
		Queue<TimestampValue> queue = null;

		counter++;
		StringBuilder key = new StringBuilder().append(sensorId).append(obsType);
		innerHashMap = valuesMap.get(key);
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
		if (obsType.equals("SLR")) { // FIXME: Make generic by using a property for values that require split!
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
}
