package in.dream_lab.bm.stream_iot.tasks.predict;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shukla, simmhan
 *
 */
public class SimpleLinearRegressionPredictor extends AbstractTask<String,float[]> {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	private static int useMsgField;

	private static int trainWindowSize;
	private static int predictionHorizonSize;

	// local fields assigned to each thread
	// maintain the last items than need to be removed from window by value
	Deque<Float[]> trailWindowItems = new ArrayDeque<>();
	SimpleRegression simpleReg;
	long itemCount = 0;
	float[] predictions;

	/**
	 * @param l_
	 * @param p_
	 */
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				 // If positive use it as index in comma sep. value else use dummyInputConst
				useMsgField = Integer.parseInt(p_.getProperty("PREDICT.SIMPLE_LINEAR_REGRESSION.USE_MSG_FIELD", "0"));

				//  count-window over input data for prediction, and number of items to predict
				trainWindowSize = Integer.parseInt(p_.getProperty("PREDICT.SIMPLE_LINEAR_REGRESSION.WINDOW_SIZE_TRAIN", "10"));
				predictionHorizonSize = Integer.parseInt(p_.getProperty("PREDICT.SIMPLE_LINEAR_REGRESSION.WINDOW_SIZE_PREDICT", "10"));
				doneSetup = true;
			}
		}
		simpleReg = new SimpleRegression();
		predictions = new float[predictionHorizonSize];
	}

	/***
	 *
	 * @param m
	 * @return
     */

	@Override
	protected Float doTaskLogic(Map map) 
	{
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		float item;
		itemCount++;

		if (useMsgField > 0) {
			item = Float.parseFloat(m.split(",")[useMsgField - 1]);
		}
		else if (useMsgField == 0)
		{
			item = Float.parseFloat(m);
		}
		
		else {
			item = itemCount + ThreadLocalRandom.current().nextInt(100);
		}
		

//		if(l.isInfoEnabled())
//		l.info("Input tuple "+m);

		// add latest <attr, value> pair to list & regression
		trailWindowItems.add(new Float[] { (float) itemCount, item });
		simpleReg.addData(itemCount, item);
//		if (l.isInfoEnabled())
//			l.info("1-syscount-{},val-{},sysVals-{}", itemCount, item, trailWindowItems);

		// make prediction once train window is full
		if (itemCount > trainWindowSize) {
			// remove latest <attr, value> pair from list & regression to maintain train window size
			Float[] oldVal = trailWindowItems.remove();
			simpleReg.removeData(oldVal[0], oldVal[1]);
//			if (l.isInfoEnabled())
//				l.info("2-syscount-{},val-{},sysVals-{}", itemCount, item, trailWindowItems);

			// make 'predictionHorizonSize' number of predictions
			StringBuffer predStr = null;
			if (l.isInfoEnabled()) 
				predStr = new StringBuffer("attribute-").append(itemCount + 1).append(',');
			double sum = 0;
			for (int j = 1; j <= predictionHorizonSize; j++) {
				double pred = simpleReg.predict(itemCount + j);
				sum += pred;
				predictions[j-1] = (float) pred;
//				if (l.isInfoEnabled()) predStr.append(pred).append(',');
			}

//			if (l.isInfoEnabled()) {
//				l.info("Next {} predictions - {}", predictionHorizonSize, predStr.toString());
//				l.info("Avg of {} predictions = {}", predictionHorizonSize, (float) (sum / predictionHorizonSize));
//			}
			
			// set parent to have the actual predictions
			super.setLastResult(predictions);
			
			// return average of the predictions
			return (float) (sum / predictionHorizonSize);
			
		} else {
			// no predictions till window is full
			return null;
		}
	}
}
