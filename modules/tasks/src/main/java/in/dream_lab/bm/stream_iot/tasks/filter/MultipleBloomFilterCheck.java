package in.dream_lab.bm.stream_iot.tasks.filter;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * @author shilpa, simmhan
 */
public class MultipleBloomFilterCheck extends AbstractTask<String,Float> {

	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static String[] useMsgFieldList;
    
	
	private static String bloomFilterFilePaths;
	private static int testingRange;
	private static HashMap <String,BloomFilter<String>> bloomFilterMap;

	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) {
				try 
				{
					// If positive, use that particular field number in the input CSV message as input for count
					String useMsgList =p_.getProperty("FILTER.MULTI_BLOOM_FILTER.USE_MSG_FIELD_LIST"); 
					bloomFilterFilePaths=(p_.getProperty("FILTER.MULTI_BLOOM_FILTER.MODEL_PATH_LIST")); // FIXME: ensure props file are updated
					int expectedInsertions = Integer.parseInt(p_.getProperty("FILTER.BLOOM_FILTER_TRAIN.EXPECTED_INSERTIONS", "20000000"));

					useMsgFieldList = useMsgList.split(",");
					String bloomFilterPathList[] = bloomFilterFilePaths.split(",");
					
					// Check to enable matching of bloom filter model files and fields provided in property file
					if(useMsgFieldList.length != bloomFilterPathList.length) {
						l.warn("Improper bloom filter msg list and path count count. " + useMsgFieldList.length + " <> " + bloomFilterPathList.length);
						doneSetup=false;
						return;
					}
					
					bloomFilterMap = new HashMap<String,BloomFilter<String>>();
					testingRange = expectedInsertions;
					BloomFilter<String> bloomFilter;
					
					/// Populating bloom filter for each model
					for(int i = 0; i < useMsgFieldList.length ;i++) {
						//Load BloomFilter from serialized file
						bloomFilter  = readBloomFilterFromfile(bloomFilterPathList[i],l);
						if(bloomFilter == null) {
							l.warn("Exception in populating model from file. " + bloomFilterPathList[i]);
							doneSetup=false;
							return;
						}
						bloomFilterMap.put(useMsgFieldList[i], bloomFilter);
						if(l.isInfoEnabled()) l.info("loaded bloom filter: " + bloomFilter);
					}	
					doneSetup = true;
				} catch (Exception e) {
					l.warn("error loading bloom filter file from: " + bloomFilterFilePaths, e);
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
		BloomFilter<String> bloomFilter;
		String obsVal ="";
		String obsType ="";
		for(int i = 0; i< useMsgFieldList.length ; i++)
		{
			if( map.containsKey(useMsgFieldList[i]) )
			{
				obsType = useMsgFieldList[i];
				obsVal = map.get(useMsgFieldList[i]);
				bloomFilter = bloomFilterMap.get(obsType);
				Boolean b = bloomFilter.mightContain(obsVal);
				if(l.isInfoEnabled()) 
					l.info("Boolean output from bloom - " + b);	
				return super.setLastResult(b ? 1f :  0f); // FIXME: handle tuple value replacement to NULL in Bolt, not task
			}
		}
		return 1f;
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
		try
		{
			FileInputStream fis = new FileInputStream(new File(bloomFilterFilePath));
			return BloomFilter.readFrom(fis, memberFunnel);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}


}
