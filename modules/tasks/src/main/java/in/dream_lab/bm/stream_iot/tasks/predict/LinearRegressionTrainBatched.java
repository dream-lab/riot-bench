package in.dream_lab.bm.stream_iot.tasks.predict;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.slf4j.Logger;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instances;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
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
public class LinearRegressionTrainBatched extends AbstractTask {

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
				modelFilePath = p_.getProperty("TRAIN.LINEAR_REGRESSION.MODEL_PATH");
//				String arffFilePath = p_.getProperty("PREDICT.LINEAR_REGRESSION.TRAIN.ARFF_PATH");
//				modelTrainFreq= Integer.parseInt(p_.getProperty("PREDICT.LINEAR_REGRESSION.TRAIN.MODEL_UPDATE_FREQUENCY"));

					// converting arff file with header only to string
//					instanceHeader = WekaUtil.readFileToString(arffFilePath, StandardCharsets.UTF_8);
					instanceHeader=p_.getProperty("PREDICT.LINEAR_REGRESSION.SAMPLE_HEADER");

					doneSetup=true;

			}
		}
		// setup for NON-static fields
		instancesCount=0;
		instancesBuf = new StringBuffer(instanceHeader);
//		try {
//			dummyData=new String(readAllBytes(Paths.get(p_.getProperty("PREDICT.LINEAR_REGRESSION.DUMMY_DATA"))));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}

	}



	@Override
	protected Float doTaskLogic(Map map) 
	{

//		m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//		System.out.println("instancesBuf-"+instancesBuf.toString());
		// code for micro benchmark : START
//		dummyData=""
//		String modelname="TEST-MLR.model"+ UUID.randomUUID();
//		map.put("FILENAME",modelname);
//		map.put(AbstractTask.DEFAULT_KEY,dummyData);
//		// code for micro benchmark : END

		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		String filename = (String)map.get("FILENAME");
		ByteArrayOutputStream model=new ByteArrayOutputStream();



		String fullFilePath=modelFilePath+filename;  //  model file updated with MLR-endRowkey.model
		int result = 0;
		try {

			instancesBuf.append("\n").append(m).append("\n");
				// train and save model
				if(l.isInfoEnabled())
					l.info("instancesBuf-"+instancesBuf.toString());
				StringReader stringReader = new StringReader(instancesBuf.toString());
				result = linearRegressionTrainAndSaveModel(stringReader, fullFilePath,model, l);

				if(l.isInfoEnabled()) {
					LinearRegression readModel = (LinearRegression) weka.core.SerializationHelper.read(fullFilePath);
					l.info("Trained Model L.R.-{}", readModel.toString());
//					System.out.println("Trained Model L.R.-{}" + readModel.toString());
				}

				super.setLastResult(model);
				instancesBuf = new StringBuffer(instanceHeader);
//			}

			if(result >= 0) return Float.valueOf(0); // success
			
		} catch (Exception e) {
			l.warn("error training decision tree", e);
		}

		return Float.valueOf(1);
//		return Float.valueOf(Float.MIN_VALUE);
	}

	/**
	 *  @param instanceReader
	 * @param modelFilePath
	 * @param model
	 * @param l  @return
	 */
	private static int linearRegressionTrainAndSaveModel(StringReader instanceReader, String modelFilePath, ByteArrayOutputStream model, Logger l){

		Instances trainingData = WekaUtil.loadDatasetInstances(instanceReader,l);
		if(trainingData == null) return -1;
		
		try {
			// train the model
			LinearRegression lr = new LinearRegression();
			lr.buildClassifier(trainingData);


			weka.core.SerializationHelper.write(model,lr);

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
