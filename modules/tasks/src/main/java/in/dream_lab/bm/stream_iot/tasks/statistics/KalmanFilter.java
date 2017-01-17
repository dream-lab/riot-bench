package in.dream_lab.bm.stream_iot.tasks.statistics;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import org.slf4j.Logger;

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
public class KalmanFilter extends AbstractTask {


	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object(); 
	private static boolean doneSetup = false;
	private static int useMsgField;

	private static float q_processNoise;
	private static float r_sensorNoise;

	// local fields assigned to each thread
	private float p0_priorErrorCovariance;
	private float x0_previousEstimation;


	/**
	 * @param l_
	 * @param p_
	 */
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) { // ONLY for static fields
			if(!doneSetup) { // Do setup only once for this task
				// If positive, use that particular field number in the input CSV message as input for count else random value
				useMsgField = Integer.parseInt(p_.getProperty("STATISTICS.KALMAN_FILTER.USE_MSG_FIELD", "0")); 
				q_processNoise= Float.parseFloat(p_.getProperty("STATISTICS.KALMAN_FILTER.PROCESS_NOISE", "0.1")); 
				r_sensorNoise= Float.parseFloat(p_.getProperty("STATISTICS.KALMAN_FILTER.SENSOR_NOISE", "0.1"));
				doneSetup=true;
			}
		}
		
		// Init Non-Static fields
		p0_priorErrorCovariance = Float.parseFloat(p_.getProperty("STATISTICS.KALMAN_FILTER.ESTIMATED_ERROR", "1"));  
	}
	
	/**
	 * Based on applying Kalman Filter for a voltage sensor at:
	 * Kalman Filter For Dummies, Bilgin Esme
	 * http://bilgin.esme.org/BitsAndBytes/KalmanFilterforDummies
	 */
	@Override
	protected Float doTaskLogic(Map map) 
	{
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		// get measure value for current iteration
		float z_measuredValue;
		if(useMsgField>0) {
			z_measuredValue = Float.parseFloat(m.split(",")[useMsgField-1]);

//			if(l.isInfoEnabled())
//			l.info("TEST1:z_measuredValue-"+z_measuredValue);
		}
		else if (useMsgField ==0)
		{
			z_measuredValue = Float.parseFloat(m);
		}
		else {
			// generate a value of 10 +/- 1.0 
			z_measuredValue = 10 + 
					ThreadLocalRandom.current().nextFloat() * (ThreadLocalRandom.current().nextBoolean() ? -1 : 1);
		}

		// Time Update
		float p1_currentErrorCovariance = p0_priorErrorCovariance + q_processNoise;
		
		// Measurement update
		float k_kalmanGain = p1_currentErrorCovariance/(p1_currentErrorCovariance + r_sensorNoise);		
		float x1_currentEstimation = x0_previousEstimation + k_kalmanGain*(z_measuredValue - x0_previousEstimation);
		p1_currentErrorCovariance = (1 - k_kalmanGain)*p1_currentErrorCovariance;

		// Log/send result
//		if(l.isInfoEnabled()) {
//			l.info("Input Value {}", z_measuredValue);
//			l.info("Kalman estimated value {}", x1_currentEstimation);
//			l.info("Kalman error cov {}", p1_currentErrorCovariance);
//		}
		Float currentEstimation = Float.valueOf(x1_currentEstimation);
		
		// Update estimate and covariance for next iteration
		x0_previousEstimation = x1_currentEstimation;
		p0_priorErrorCovariance = p1_currentErrorCovariance;

		return Float.valueOf(currentEstimation);
	}




}
