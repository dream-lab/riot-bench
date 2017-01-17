package in.dream_lab.bm.stream_iot.tasks.filter;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.Defaults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

/**
 * @author shilpa 8/07/16 
 * Sample range filter 1:10:100,2:20:30.5,3:5:10 <fieldnum : min : max>
 * 
 * FIXME: Are we using message fields? Should we just move to Map for all cases? i.e. replace msgField with obsName? 
 */
public class RangeFilterCheck extends AbstractTask<String,Float> {
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private Map<String, MinMax> validRangeMap;
	private ArrayList<String>  useMsgFieldList;
	
 	/**
	 * Helper class
	 * @author simmhan
	 *
	 */
	class MinMax {
		float min, max;

		public MinMax(String min_, String max_) {
			min = Float.parseFloat(min_);
			max = Float.parseFloat(max_);
		}
	}
	
	@Override
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK)
		{
				if (!doneSetup) 
				{
					doneSetup = true;
				}
		}
				// read min and max value for multiple fields into hashmap
				useMsgFieldList = new ArrayList<String>();
				validRangeMap = new HashMap<String, MinMax>();
				String rangeValue = p_.getProperty("FILTER.RANGE_FILTER.VALID_RANGE");
				String[] rangeValues = rangeValue.split(",");
				String temp[];

				// populate the hash map with min max value separated by ':' as value and field name is key
				for (String s : rangeValues) {
					temp = s.split(":");
					if (temp.length != 3) {
						l.warn("error with range entry: " + s); 
						doneSetup=false;
					} else 
						try {
							useMsgFieldList.add(temp[0]);
							validRangeMap.put(temp[0], new MinMax(temp[1],temp[2]));
						} catch(NumberFormatException nfex) {
							l.warn("error parsing Float values for range entry: " + s); 
							doneSetup=false;							
						}
				}
		}

	@Override
	protected Float doTaskLogic(Map<String, String> map) 
	{	
		String obsType ;
		String obsVal ;
		boolean isValid = true;
		for(String msgField : useMsgFieldList)
		{
			if(map.containsKey(msgField))
			{
				obsType = msgField;
				obsVal = map.get(msgField);
				
				/* Fetch min max values */
				MinMax minMax = validRangeMap.get(obsType);
				if (minMax != null) {
					// check outlier values
					try { // FIXME: Why using Double here and Float for range Map?
						if (Float.parseFloat(obsVal) < minMax.min || Float.parseFloat(obsVal) > minMax.max) 
							isValid = false;
					} catch(NumberFormatException nfex) {
						l.warn("error parsing Double values for tuple value " + obsVal + " of type " + obsType); 
						doneSetup=false;							
					}
				}
			}
		}
		return super.setLastResult(isValid ? 1f : 0f); // FIXME: handle tuple value replacement to NULL in Bolt, not task
	}

}