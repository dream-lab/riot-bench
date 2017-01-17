package in.dream_lab.bm.stream_iot.tasks.statistics;

import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;



/**
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shukla, simmhan
 *
 */
public class SecondOrderMoment extends AbstractTask {


	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object(); 
	private static boolean doneSetup = false;
	private static int useMsgField;

	private static int maxMapSize;
	
	// local fields assigned to each thread
	private Map<Float, Integer> itemToFreqArrayIndexMap;
	private int freq[];
	private float items[];
	private long counter;
	private int nextAvailIndex = 0;

	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) { // for static fields
			if (!doneSetup) { // Do setup only once for this task
				useMsgField = Integer.parseInt(p_.getProperty("STATISTICS.MOMENT.USE_MSG_FIELD", "0"));
				maxMapSize = Integer.parseInt(p_.getProperty("STATISTICS.MOMENT.MAX_HASHMAPSIZE", "100000"));
				doneSetup = true;
			}
		}
		
		// setup for NON-static fields
		itemToFreqArrayIndexMap = new HashMap<Float, Integer>();
		freq = new int[maxMapSize];
		items = new float[maxMapSize];
	}

	/**
	 * @param m
	 * @return
	 */
	@Override
	protected Float doTaskLogic(Map map) 
	{
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		// second moment or Distribution is to be found
		float item;
		if (useMsgField > 0) {
			item = Float.parseFloat(m.split(",")[useMsgField - 1]);
		} else {
			item = ThreadLocalRandom.current().nextFloat();
		}
		
		counter++;
		float result = doMomentOp(counter, item, itemToFreqArrayIndexMap, items, freq, nextAvailIndex, l);
		if (itemToFreqArrayIndexMap.size() < maxMapSize)
			nextAvailIndex++;

		if (l.isInfoEnabled())
			l.info("Result of SecondOrderMoment op-" + result);

//		return (float) result;
		return  (Float)super.setLastResult(result);
	}

	/**
	 * Calculating Moments by Alon-Matias-Szegedy Algorithm for second order moment The space complexity of
	 * approximating the frequency moments . Larger the 2nd moment value more non-uniform will be the distribution
	 * 
	 * @param counter
	 *            items seen thus far
	 * @param item
	 *            value of current item being added
	 * @param itemToIndexMap
	 *            Map from item to index in the freq[]/items[] array having values for that item
	 * @param freq
	 *            freq[] aray holding frequency count of how often the item was seen
	 * @param nextAvailIndex
	 * @param l
	 * @return
	 */
	static float doMomentOp(long counter, float item, 
			Map<Float, Integer> itemToIndexMap, float[] items, int[] freq,
			int nextAvailIndex, Logger l) {

		if (l.isInfoEnabled()) {// print the Map
			for (Float key : itemToIndexMap.keySet()) {
				l.info("Key: {} , ArrayIndex: {}, Freq: {}", key, itemToIndexMap.get(key), freq[itemToIndexMap.get(key)]);
			}
		}

		if (!itemToIndexMap.containsKey(item)) { // input item not present
			if (itemToIndexMap.size() < maxMapSize) { // have spare capacity, add new item to end of freq array
				itemToIndexMap.put(item, nextAvailIndex);
				freq[nextAvailIndex] = 1;
				items[nextAvailIndex] = item;
				if (l.isInfoEnabled())
					l.info("CASE1:New entry {}", item);
			} else { // dont have spare capacity, replace random old item by new item
				// pick a random item to remove from items[] array
				int rndItemIndex = ThreadLocalRandom.current().nextInt(maxMapSize);
				float rndItem = items[rndItemIndex];
				int replacementIndex = itemToIndexMap.get(rndItem);
				itemToIndexMap.remove(rndItem);
				itemToIndexMap.put(item, replacementIndex);
				freq[replacementIndex] = 1;
				items[replacementIndex] = item;
				if (l.isInfoEnabled())
					l.info("CASE2:Reached max size, replacing rnd item {} with new item {}", rndItem, item);
			}
		} else { // element-key is already in Map
			int itemIndex = itemToIndexMap.get(item);
			freq[itemIndex]++;
			if (l.isInfoEnabled())
				l.info("CASE3:Key {} is already in map, freq is {}", item, freq[itemIndex]);
		}

		// calculate the 2nd order moment using Alon-Matias-Szegedy Algorithm
		if (l.isInfoEnabled()) l.info("TEST array {}", Arrays.toString(freq));

		float surpriseNumber = 0;
		for (int countOfElement : freq) {
			surpriseNumber += counter * (2 * countOfElement - 1);
			if (l.isInfoEnabled()) l.info("{},{},{}", counter, countOfElement, surpriseNumber);
		}
		surpriseNumber = surpriseNumber / itemToIndexMap.size();
		
		return surpriseNumber;
	}

}
