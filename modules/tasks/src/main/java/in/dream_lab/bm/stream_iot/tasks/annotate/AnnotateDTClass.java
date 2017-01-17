package in.dream_lab.bm.stream_iot.tasks.annotate;

import com.google.common.primitives.Doubles;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author shilpa
 *
 */
public class AnnotateDTClass extends AbstractTask<String,String>
{
	private static TreeMap<Double, String> annotationMap;
	private static String filePath ; 
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int useMsgField;
	//private String sampleData = "024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14 03:57:00,2013-01-14 04:23:00,200,10,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,0.00,0.50,13.00,4.80,70.30";
	
	@Override
	public void setup(Logger l_, Properties p_) 
	{
		super.setup(l_, p_);
			synchronized (SETUP_LOCK)
			{
				if(!doneSetup) 
				{
					// Check usefield 
					useMsgField = Integer.parseInt(p_.getProperty("ANNOTATE.ANNOTATE_MSG_USE_FIELD", "0"));
		    		doneSetup = true;
				}
			}
	}

	@Override
	protected Float doTaskLogic(Map<String,String> map)
	{

		List<Double> doubleValues = new ArrayList<>();

		String result=map.get(AbstractTask.DEFAULT_KEY);
//		 Loop through the results, displaying information about the entity
		for (String entity : result.split("\n")) {
//			if(l.isInfoEnabled())
//				l.info("val at annotate {}",entity);
			doubleValues.add(Double.valueOf(entity.split(",")[useMsgField]));// 4 for sys
		}

		double[] data = Doubles.toArray(doubleValues);
		// The data must be ordered
		Arrays.sort(data);
		Percentile percentile = new Percentile();
		percentile.setData(data);

//		System.out.println("percentile results:"+percentile.evaluate(100));

		if(l.isInfoEnabled())
			l.info("percentile results: {}",percentile.evaluate(100));

		annotationMap = new TreeMap<Double, String>();
		annotationMap.put(Double.valueOf(percentile.evaluate(25)), "BAD");
		annotationMap.put(Double.valueOf(percentile.evaluate(50)), "GOOD");
		annotationMap.put(Double.valueOf(percentile.evaluate(75)), "VERYGOOD");
		annotationMap.put(Double.valueOf(percentile.evaluate(100)), "EXCELLENT");

		if(l.isInfoEnabled()){
			for(Map.Entry<Double, String> tm : annotationMap.entrySet())
			{   //print keys and values
				System.out.println(tm.getKey() + " : " +tm.getValue());
			}
		}


		StringBuffer annotatedData=new StringBuffer();
		for (String entity : result.split("\n")) {
//			if(l.isInfoEnabled())
//				l.info("val at annotate {}",entity.split(",")[useMsgField]);
			Double in =Double.valueOf(entity.split(",")[useMsgField]);

//			System.out.println("in:"+in);
			Double annotateKey = annotationMap.floorKey(in);
//			System.out.println("annotateKey:"+annotateKey);
			if(annotateKey==null && in<annotationMap.firstKey()){
				annotateKey= annotationMap.firstKey();
			}

			// Fetch annotated values from hashmap corresponding to this key
			String annotation = annotationMap.get(annotateKey);
//			System.out.println("Below:"+annotateKey+","+annotation);

			annotatedData.append(entity).append(",").append(annotation).append("\n");
		}

		if(l.isInfoEnabled())
			l.info("annotated data is {}",annotatedData);

		super.setLastResult(annotatedData.toString());

		return 0f;
	}
	
	
}
