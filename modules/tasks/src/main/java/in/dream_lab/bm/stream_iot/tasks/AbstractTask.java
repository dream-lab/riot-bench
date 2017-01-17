package in.dream_lab.bm.stream_iot.tasks;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Properties;

public abstract class AbstractTask<T, U> implements ITask<T, U> {

	protected Logger l;
	protected StopWatch sw;
	protected int counter;
	private U lastResult = null;
	public final static String DEFAULT_KEY = "D";
	
	
	@Override
	public void setup(Logger l_, Properties p_) {
		l = l_;
		sw = new StopWatch();
		sw.start();
		sw.suspend();
		counter = 0;
		l.debug("finished task setup");
	}
	
	
	/**
	 * Increments timer and counter for number of calls. Calls child class. 
	 * Returns a float value indicating the status or value of the execution.
	 * Float.MIN_FLOAT means there was an error. All other values are valid. 
	 * Null return means there is no output to pass on. 
	 */
	@Override
	public Float doTask(Map<String, T> map) {
		sw.resume();
		////////////////////////
		Float result = doTaskLogic(map);
		////////////////////////
		sw.suspend();
		assert result >= 0;
		counter++;		
		return result;
	}
	
	/**
	 * Returns the last result that was set by the child task in case there is a
	 * result other than the float returned by doTaskLogic() 
	 *  
	 * @return
	 */
	public U getLastResult() {
		return lastResult;
	}
	
	/**
	 * Set by child task if there are results other than the float 
	 * result that need to be accessed by invoker. 
	 * 
	 * @param r
	 */
	protected U setLastResult(U r) {
		lastResult = r;
		return lastResult;
	}
	
	/**
	 * To be implemented by child class
	 * 
	 * @param map
	 * @return
	 */
	protected abstract Float doTaskLogic(Map<String, T> map);
	
	
	@Override
	public float tearDown() {
		sw.stop();
		l.debug("finished task tearDown");
		return sw.getTime()/counter;
	}
	

}
