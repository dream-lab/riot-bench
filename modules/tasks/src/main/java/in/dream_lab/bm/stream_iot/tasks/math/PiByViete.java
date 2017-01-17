package in.dream_lab.bm.stream_iot.tasks.math;

import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shukla, simmhan
 *
 */
public class PiByViete extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	private static int iterations;
	
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				// pass number of iters, or use default
				iterations = Integer.parseInt(p_.getProperty("MATH.PI_VIETE.ITERS", "1600"));
				assert iterations > 0;
				
				doneSetup=true;
			}
		}
	}
	
	@Override
	protected Float doTaskLogic(Map map) 
	{
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		// PI calc. is independent of m input here
		float result = getPiByViete(iterations, l);
		if(l.isInfoEnabled()) l.info("Result by Pi Op {} ", result);
		return result;
	}

	/**
	 * https://en.wikipedia.org/wiki/Vi%C3%A8te%27s_formula
	 * http://www.codeproject.com/Articles/813185/Calculating-the-Number-PI-Through-Infinite-Sequenc
	 * 
	 * @param n
	 * @param l
	 * @return
	 */
	public static float getPiByViete(int n, Logger l) {

		double i, j; // Number of iterations and control variables
		double f; // factor that repeats
		double pi = 1;

		for (i = n; i > 1; i--) {
			f = 2;
			for (j = 1; j < i; j++) {
				f = 2 + Math.sqrt(f);
			}
			f = Math.sqrt(f);
			pi = pi * f / 2;
		}
		pi *= Math.sqrt(2) / 2;
		pi = 2 / pi;
		return (float) pi;
	}


}
