package in.dream_lab.bm.stream_iot.tasks.predict;


import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import org.slf4j.Logger;

import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shukla, simmhan
 *
 */
public class DecisionTreeClassify extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	private static int useMsgField;
	
	// Sample data, assuming arff file has headers for Sense-Your-City dataset
	private static final String SAMPLE_INPUT = "-71.10,42.37,10.1,65.3,0";
	// for taxi dataset
//	private static final String SAMPLE_INPUT = "420,1.95,8.00";
//	// Encode the arff header for SYS as a constant string
	private static  String SAMPLE_HEADER ="";

//			"@RELATION SYS_data\n" +
//			"\n" +
////			"@ATTRIBUTE Longi            NUMERIC\n" +
////			"@ATTRIBUTE Lat              NUMERIC\n" +
//			"@ATTRIBUTE Temp             NUMERIC\n" +
//			"@ATTRIBUTE Humid            NUMERIC\n" +
//			"@ATTRIBUTE Light            NUMERIC\n" +
//			"@ATTRIBUTE Dust             NUMERIC\n" +
//			"@ATTRIBUTE airquality       NUMERIC\n" +
//			"@ATTRIBUTE result           {Bad,Average,Good,VeryGood,Excellent}\n" +
//			"\n" +
//			"@DATA\n" +
//			"%header format";

//	// Sample data, assuming arff file has headers for TAXI dataset

//	// Encode the arff header for SYS as a constant string
//	private static final String SAMPLE_HEADER = "@RELATION SYS_data\n" +
//			"\n" +
//			"@ATTRIBUTE triptimeInSecs            NUMERIC\n" +
//			"@ATTRIBUTE tripDistance             NUMERIC\n" +
//			"@ATTRIBUTE fareAmount       NUMERIC\n" +
//			"@ATTRIBUTE result           {Bad,Good,VeryGood}\n" +
//			"\n" +
//			"@DATA\n" +
//			"%header format";

	private static Instances instanceHeader; 
	private static int resultAttrNdx;  // Index of result attribute in arff file
	public static J48 j48tree;
	
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				// If positive, use actual tuple as input else SAMPLE_INPUT
				useMsgField = Integer.parseInt(p_.getProperty("CLASSIFICATION.DECISION_TREE.USE_MSG_FIELD", "0")); 
				String modelFilePath = p_.getProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH");
				// attribute index for getting the resulting enum
				resultAttrNdx = Integer.parseInt(p_.getProperty("CLASSIFICATION.DECISION_TREE.CLASSIFY.RESULT_ATTRIBUTE_INDEX")); 
				try {
					j48tree = (J48) weka.core.SerializationHelper.read(modelFilePath);
					if(l.isInfoEnabled()) l.info("Model is {}", j48tree);

					SAMPLE_HEADER=p_.getProperty("CLASSIFICATION.DECISION_TREE.SAMPLE_HEADER");
					instanceHeader = WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER), l);
					if(l.isInfoEnabled()) l.info("Header is {}", instanceHeader);
					assert instanceHeader != null;
					
					doneSetup=true;
				} catch (Exception e) {
					l.warn("error loading decision tree model from file: "+modelFilePath, e);
					doneSetup=false;
				}
			}
		}
	}


	@Override
	protected Float doTaskLogic(Map map) 
	{
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		Instance testInstance = null;
		try {
			String[] testTuple = null;
			if(useMsgField > 0) {// useMsgField is used as flag
				testTuple = m.split(",");
			}
			else{
//				System.out.println("TestS : in do task" );
				testTuple = SAMPLE_INPUT.split(",");
			}
			testInstance = WekaUtil.prepareInstance(instanceHeader, testTuple, l);
			int classification = (int)j48tree.classifyInstance(testInstance);
			System.out.println("DT result from task  " +classification);
			String result = instanceHeader.attribute(resultAttrNdx-1).value(classification);
			System.out.println("DT result from task  " +result);
			if(l.isInfoEnabled()) {
				l.info(" ----------------------------------------- ");
				l.info("Test data               : {}", testInstance);
				l.info("Test data classification result {}, {}", result , classification);

			}
			return Float.valueOf(classification);
		} catch (Exception e) {
			l.warn("error with clasification of testInstance: " + testInstance, e);
			return Float.valueOf(Float.MIN_VALUE);
		}
	}



}
