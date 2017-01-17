package in.dream_lab.bm.stream_iot.tasks.predict;


import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

import weka.classifiers.trees.J48;
import weka.core.Instances;

/**
 * This task should only be run from a single thread to avoid overwriting output model file.
 *  
 * @author shukla, simmhan
 *
 */
public class DecisionTreeTrain extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	
	private static String modelFilePath;
	private static int modelTrainFreq;
	private static String instanceHeader;

	// local fields assigned to each thread
	private int instancesCount = 0;
	private StringBuffer instancesBuf = null;

	public void setup(Logger l_, Properties p_) { 
		// TODO: Later, have option of training using instances present in data file rather than just from messages
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				modelFilePath = p_.getProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH");
				String arffFilePath = p_.getProperty("CLASSIFICATION.DECISION_TREE.ARFF_PATH");
				modelTrainFreq = Integer.parseInt(p_.getProperty("CLASSIFICATION.DECISION_TREE.TRAIN.MODEL_UPDATE_FREQUENCY"));
				try {
					// converting arff file with header only to string
					instanceHeader = WekaUtil.readFileToString(arffFilePath, StandardCharsets.UTF_8);
					doneSetup=true;
				} catch (IOException e) {
					l.warn("error reading arff file: " + arffFilePath, e);
					doneSetup=false;
				}
			}
		}
		// setup for NON-static fields
		instancesCount=0;
		instancesBuf = new StringBuffer(instanceHeader);
	}


	@Override
	protected Float doTaskLogic(Map map) 
	{
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		int result = 0;
		try {
			instancesCount++;
			instancesBuf.append(m).append("\n");

			
			if(instancesCount == modelTrainFreq){ // instances count is full
				l.warn("instancesBuf-"+instancesBuf.toString());
				// train and save model
				StringReader stringReader = new StringReader(instancesBuf.toString());
			    result = decisionTreeTrainAndSaveModel(stringReader, modelFilePath, l);
			    
			    // reset instances
				instancesCount = 0;
				instancesBuf = new StringBuffer(instanceHeader);
			}
			
			//			l.info("result returned -"+result);
			if(result >= 0) return Float.valueOf(result); // success
			
		} catch (Exception e) {
			l.warn("error training decision tree", e);
		}
		return Float.valueOf(Float.MIN_VALUE);
	}


	/***
	 *
	 * @param instancesReader
	 * @param modelFilePath
	 * @param l
     * @return
     */
	private static int decisionTreeTrainAndSaveModel(StringReader instancesReader,String modelFilePath, Logger l){

		Instances trainingData = WekaUtil.loadDatasetInstances(instancesReader, l);
		if(trainingData == null) return -1;

		try {
			// train the model
			J48 j48tree = new J48();
			j48tree.buildClassifier(trainingData);
			if(l.isInfoEnabled()) l.info("Model is - "+j48tree.toString());

			System.out.println(("Model is - "+j48tree.toString()));
			
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
