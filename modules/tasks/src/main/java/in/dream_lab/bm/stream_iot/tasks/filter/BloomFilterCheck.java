package in.dream_lab.bm.stream_iot.tasks.filter;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BloomFilterCheck extends AbstractTask<String,Float> {

	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int useMsgField;

	private static String bloomFilterFilePath;
	private static int testingRange;
	private static BloomFilter<String> bloomFilter;

	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) {
				// If positive, use that particular field number in the input CSV message as input for count
				useMsgField = Integer.parseInt(p_.getProperty("FILTER.BLOOM_FILTER_CHECK.USE_MSG_FIELD", "0")); 
				System.out.println("use msg field value " + useMsgField);
				bloomFilterFilePath=(p_.getProperty("FILTER.BLOOM_FILTER.MODEL_PATH"));
				int expectedInsertions = Integer.parseInt(p_.getProperty("FILTER.BLOOM_FILTER_TRAIN.EXPECTED_INSERTIONS", "20000000"));
				
				// autogenerate random values with range that is same as the number of insertions
				// so that each item has a 50% chance of getting rejected by filter
				testingRange = expectedInsertions;
				
				//Load BloomFilter from serialized file
				try {
					bloomFilter = readBloomFilterFromfile(bloomFilterFilePath,l);
					if(l.isInfoEnabled()) l.info("loaded bloom filter: " + bloomFilter);

					doneSetup = true;
				} catch (IOException e) {
					l.warn("error loading bloom filter file from: " + bloomFilterFilePath, e);
					doneSetup=false;
				}
			}
		}
	}

	
	/**
	 * Implementation of Bloom filter
	 * https://google.github.io/guava/releases/snapshot/api/docs/com/google/common/hash/BloomFilter.html
	 */
	@Override
	protected Float doTaskLogic(Map<String,String> map) 
	{
		String m = map.get(AbstractTask.DEFAULT_KEY);
		String in = null;
		if(useMsgField>0) {
			in =(m.split(",")[useMsgField-1]);
		}
		else{
			in = String.valueOf(ThreadLocalRandom.current().nextInt(testingRange));
		}
		Boolean b = bloomFilter.mightContain(in);
		if(l.isInfoEnabled()) {
			l.info("Boolean output from bloom - " + b);
		}		
		return super.setLastResult(b ? 1f :  0f);
	}

	
	/***
	 *
	 * @param bloomFilterFilePath
	 * @param l
	 * @return
     * @throws IOException
     */
	static BloomFilter<String> readBloomFilterFromfile(String bloomFilterFilePath, Logger l) throws IOException {
		Funnel<String> memberFunnel = new Funnel<String>() {
			public void funnel(String memberId, PrimitiveSink sink) {
				sink.putString(memberId, Charsets.UTF_8);
			}
		};

		FileInputStream fis = new FileInputStream(new File(bloomFilterFilePath));
		return BloomFilter.readFrom(fis, memberFunnel);
	}


}
