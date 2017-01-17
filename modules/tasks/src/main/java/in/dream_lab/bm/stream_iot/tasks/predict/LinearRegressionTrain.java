package in.dream_lab.bm.stream_iot.tasks.predict;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import org.slf4j.Logger;

import weka.classifiers.functions.LinearRegression;
import weka.core.Instances;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * This task should only be run from a single thread to avoid overwriting output model file.
 *  
 * @author shukla, simmhan
 *
 */
public class LinearRegressionTrain extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	
	private static String modelFilePath;
	private static int modelTrainFreq;
	private static String instanceHeader = null;

	// local fields assigned to each thread
	private int instancesCount = 0;
	private StringBuffer instancesBuf = null;
	
	public void setup(Logger l_, Properties p_) {
		// TODO: Later, have option of training using instances present in data file rather than just from messages
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				modelFilePath = p_.getProperty("PREDICT.LINEAR_REGRESSION.MODEL_PATH");
				String arffFilePath = p_.getProperty("PREDICT.LINEAR_REGRESSION.TRAIN.ARFF_PATH");
				modelTrainFreq= Integer.parseInt(p_.getProperty("PREDICT.LINEAR_REGRESSION.TRAIN.MODEL_UPDATE_FREQUENCY"));
				
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

//		m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//		System.out.println("instancesBuf-"+instancesBuf.toString());
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		if(l.isInfoEnabled())
			l.info("Range query res:{}",m);

		int result = 0;
		try {
			instancesCount++;
			instancesBuf.append(m).append("\n");
			if(instancesCount == modelTrainFreq){  // instances count is full
				// train and save model
				l.info("instancesBuf-"+instancesBuf.toString());
				StringReader stringReader = new StringReader(instancesBuf.toString());
				result = linearRegressionTrainAndSaveModel(stringReader, modelFilePath, l);

//				if(l.isInfoEnabled()) {
					LinearRegression readModel = (LinearRegression) weka.core.SerializationHelper.read(modelFilePath);
					l.info("Trained Model L.R.-{}", readModel.toString());
					System.out.println("Trained Model L.R.-{}"+readModel.toString());

//				}

				instancesCount = 0;
				instancesBuf = new StringBuffer(instanceHeader);
			}

			if(result >= 0) return Float.valueOf(0); // success
			
		} catch (Exception e) {
			l.warn("error training decision tree", e);
		}
		
		return Float.valueOf(Float.MIN_VALUE);
	}

	/**
	 *
	 * @param instanceReader
	 * @param modelFilePath
	 * @param l
     * @return
     */
	private static int linearRegressionTrainAndSaveModel(StringReader instanceReader, String modelFilePath,Logger l){

		Instances trainingData = WekaUtil.loadDatasetInstances(instanceReader,l);
		if(trainingData == null) return -1;
		
		try {
			// train the model
			LinearRegression lr = new LinearRegression();
			lr.buildClassifier(trainingData);

			//saving the model
			weka.core.SerializationHelper.write(modelFilePath, lr);
			
		} catch (Exception e) {
			l.warn("error training linear regression", e);
			return -2;
		}
		if(l.isInfoEnabled())
			l.info("linear regression Model trained over full ARFF file and saved at {} ", modelFilePath);
		return 0;
	}

}
