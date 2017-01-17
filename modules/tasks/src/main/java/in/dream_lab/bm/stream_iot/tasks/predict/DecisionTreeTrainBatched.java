package in.dream_lab.bm.stream_iot.tasks.predict;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.slf4j.Logger;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.trees.J48;
import weka.core.Instances;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static java.nio.file.Files.readAllBytes;


/**
 * This task should only be run from a single thread to avoid overwriting output model file.
 *  
 * @author shukla, simmhan
 *
 */
public class DecisionTreeTrainBatched extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	
	private static String modelFilePath;
//	private static int modelTrainFreq;
	private static String instanceHeader = null;
	private static String SAMPLE_HEADER ="";
	// local fields assigned to each thread
	private int instancesCount = 0;
	private StringBuffer instancesBuf = null;

	private static String dummyData;
	
	public void setup(Logger l_, Properties p_) {
		// TODO: Later, have option of training using instances present in data file rather than just from messages
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				modelFilePath = p_.getProperty("TRAIN.DECISION_TREE.MODEL_PATH");
//				modelTrainFreq= Integer.parseInt(p_.getProperty("PREDICT.DECISION_TREE.TRAIN.MODEL_UPDATE_FREQUENCY"));
					// converting arff file with header only to string
//					instanceHeader = WekaUtil.readFileToString(arffFilePath, StandardCharsets.UTF_8);
					instanceHeader=p_.getProperty("CLASSIFICATION.DECISION_TREE.SAMPLE_HEADER");

					doneSetup=true;

			}
		}
		// setup for NON-static fields
		instancesCount=0;
		instancesBuf = new StringBuffer(instanceHeader);

//		try {
//			dummyData=new String(readAllBytes(Paths.get(p_.getProperty("CLASSIFICATION.DECISION_TREE.DUMMY_DATA"))));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}



	@Override
	protected Float doTaskLogic(Map map) 
	{

//		m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//		String modelname="TEST-DTC.model"+ UUID.randomUUID();
//		map.put("FILENAME",modelname);
//		map.put(AbstractTask.DEFAULT_KEY,dummyData);

		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		String filename = (String)map.get("FILENAME");
		ByteArrayOutputStream model=new ByteArrayOutputStream();
		if(l.isInfoEnabled())
			l.info("Range query res:{}",m);

		String fullFilePath=modelFilePath+filename;  //  model file updated with MLR-endRowkey.model
		int result = 0;
		try {

			instancesBuf.append("\n").append(m).append("\n");
				// train and save model
				l.info("instancesBuf-"+instancesBuf.toString());
				StringReader stringReader = new StringReader(instancesBuf.toString());
				result = decisionTreeTrainAndSaveModel(stringReader, fullFilePath,model, l);

				if(l.isInfoEnabled()) {
					l.info("Trained Model L.R.-{}", weka.core.SerializationHelper.read(fullFilePath).toString());
				}

				super.setLastResult(model);
				instancesBuf = new StringBuffer(instanceHeader);

			if(result >= 0) return Float.valueOf(0); // success
			
		} catch (Exception e) {
			l.warn("error training decision tree", e);
		}
		
		return Float.valueOf(Float.MIN_VALUE);
	}

	/***
	 *
	 * @param instancesReader
	 * @param modelFilePath
	 * @param model
	 *@param l  @return
	 */
	private static int decisionTreeTrainAndSaveModel(StringReader instancesReader, String modelFilePath, ByteArrayOutputStream model, Logger l){

		Instances trainingData = WekaUtil.loadDatasetInstances(instancesReader, l);
		if(trainingData == null) return -1;

		try {
			// train the model
			J48 j48tree = new J48();
			j48tree.buildClassifier(trainingData);
			if(l.isInfoEnabled()) l.info("Model is - "+j48tree.toString());

//			System.out.println(("Model is - "+j48tree.toString()));

			//saving the model
			weka.core.SerializationHelper.write(modelFilePath, j48tree);

		} catch (Exception e) {
			l.warn("error training decision tree", e);
			return -2;
		}

		if(l.isInfoEnabled()) {
			l.info("Decision tree Model trained and saved at {} ", modelFilePath);
			l.info("number of training instances {} ", trainingData.numInstances());
		}

		return trainingData.numInstances(); // return number of instances trained on
	}

}
