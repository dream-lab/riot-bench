package in.dream_lab.bm.stream_iot.tasks.aggregate;



import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shilpa, shukla, simmhan
 *
 */
public class DistinctApproxCount extends AbstractTask<String,Float> {

	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int k_bucketParam;
	private static int m_numBuckets;
	private static int useMsgField;

	// local fields assigned to each thread
	private int [] M_maxZeros;

	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				k_bucketParam = Integer.parseInt(p_.getProperty("AGGREGATE.DISTINCT_APPROX_COUNT.BUCKETS", "10"));
				// should not overflow for 2^bucketParam using int type whose max is 2^31 - 1
				assert k_bucketParam <= 31;  
				m_numBuckets = 1 << k_bucketParam;
				// If positive, use that particular field number in the input CSV message as input for count
				useMsgField = Integer.parseInt(p_.getProperty("AGGREGATE.DISTINCT_APPROX_COUNT.USE_MSG_FIELD", "0")); 
				doneSetup=true;
			}
		}
		// setup for NON-static fields
		M_maxZeros = new int[m_numBuckets];
	}


	@Override
	protected Float doTaskLogic(Map<String,String> map) 
	{
		String m = map.get(AbstractTask.DEFAULT_KEY);
		String item ;
		if(useMsgField > 0) {
			item = m.split(",")[useMsgField-1];
		}
		else if(useMsgField == 0)
		{
			item = m;
		}
		else{ // else, use the random long input
			item = String.valueOf(ThreadLocalRandom.current().nextLong());
		}
		int result = doUniqueCount(item, l);
//		System.out.println("result in distinct " +result);
		return super.setLastResult((float)result);
	}


	/**
	 * Gives the appox count of the number of distinct items that have been seen this far
	 * 
	 * Based on the Durand-Flajolet Algorithm
	 * [1] Loglog Counting of Large Cardinalities, Durand and Flajolet, Sec 2 
	 * http://algo.inria.fr/flajolet/Publications/DuFl03-LNCS.pdf
	 * [2] Mining of Massive Datasets, Jure Leskovec, et al, 2014: Sec 4.4.2 The Flajolet-Martin Algorithm
	 * 
	 * @param item
	 * @param l 
	 * @return E estimate of the cardinality of unique items in the stream, seen thus far
	 */
	private int doUniqueCount(String item, Logger l){

		////// Durand-Flajolet magic number statistically derived in [1] 
		// alpha
		double a_magicNum = 0.79402;   

		////// 
		// for x = b_1 b_2 ... ∈ MM
		HashFunction hf = Hashing.sha1();
		HashCode hc = hf.hashString(item, Charset.defaultCharset());
		int x_hashValue = hc.asInt();

		////// 
		// set j := <b_1 ... b_k>_2 (value of first k bits in base 2) // TODO: Check that we mean the LSB from 1--k and not MSB 
		int j_bucketId = x_hashValue & (m_numBuckets - 1);
		
		////// 
		// set M(j) := max(M(j), ρ(b_k+1 b_k+2 ... );
		M_maxZeros[j_bucketId] = Math.max(M_maxZeros[j_bucketId], p_countTrailZeros(x_hashValue >> k_bucketParam));

		////////  Cardinality estimate E from [1]
		// Sum_j(M(j))  from [1]
		double sum_M_sumMaxZeros = 0;
		for(int i = 0; i < m_numBuckets; i++){
			sum_M_sumMaxZeros += M_maxZeros[i];
		}

		// E := α * m * 2^(1/m * Sum_j(M(j)))
		int E = (int) (a_magicNum * m_numBuckets * Math.pow(2, sum_M_sumMaxZeros / m_numBuckets));

//		if(l.isInfoEnabled())
			//l.info("doUniqueCount:"+E);

		return E;
	}

	/***
	 * Returns ρ(y), the rank of first 1-bit from the left in y; // TODO: Check that we mean the first 1 from LSB and not MSB
	 * 
	 * @param y
	 * @return the rank of first 1-bit from the LSB
     */
	private static int p_countTrailZeros(int y){ // rho function from [1] 
		if(y == 0)
			return 31; // Set to 31 bits to avoid overflow of maxZeros array
		int p = 0;
		while( ((y >> p) & 1) == 0 ){
			p++;
		}
		return p;
	}
}
