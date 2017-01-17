package in.dream_lab.bm.stream_iot.tasks.aggregate;


import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shilpa, shukla, simmhan
 *
 */
public class BlockWindowAverage extends AbstractTask<String,Float> {

	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object(); 
	private static boolean doneSetup = false;
	
	private static float aggCountWindowSize=0;
	private static int useMsgField;

	// local fields assigned to each thread
	private float aggCount;
	private float aggSum;
	private float avgRes;

	
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) { // for static fields
			if(!doneSetup) { // Do setup only once static objects for this task
				aggCountWindowSize = Integer.parseInt(p_.getProperty("AGGREGATE.BLOCK_COUNT.WINDOW_SIZE")); // TODO: Later, rename to BLOCK_AVG
				useMsgField = Integer.parseInt(p_.getProperty("AGGREGATE.BLOCK_COUNT.USE_MSG_FIELD"));
				doneSetup=true;
			}
		}
	}


	@Override
	protected Float doTaskLogic(Map<String,String> map) 
	{
		String m = map.get(AbstractTask.DEFAULT_KEY);
		float item;
		if(useMsgField>0){
			item= Float.parseFloat(m);
		}
		else{
			item = ThreadLocalRandom.current().nextFloat();
		}

		aggSum+=item;
		aggCount++;
		if(aggCount<aggCountWindowSize){
//			return null;    //TODO: check for null while return in bolt code
			return super.setLastResult(null);
		}
		else {
			avgRes=Float.valueOf(aggSum/aggCount);
			aggCount = 0;
			aggSum=0;
//			l.info("CHECK: Block Average = " +avgRes);
			
			return super.setLastResult((float)avgRes);
		}
	}

}
