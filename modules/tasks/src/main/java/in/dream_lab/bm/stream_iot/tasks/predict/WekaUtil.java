package in.dream_lab.bm.stream_iot.tasks.predict;

import org.slf4j.Logger;
import weka.core.Instance;
import weka.core.Instances;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WekaUtil {

	/***
	 *
	 * @param path
	 * @param encoding
	 * @return
	 * @throws IOException
    */
	public static String readFileToString(String path, Charset encoding)
			throws IOException
	{
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding).intern();
	}
	
	/***
	 *
	 * @param instanceHeader
	 * @param testTuple
	 * @param l
    * @return
    */
	public static Instance prepareInstance(Instances instanceHeader, String[] testTuple, Logger l) {
		Instance instance = new Instance(testTuple.length);
		instance.setDataset(instanceHeader);

		for(int m=0;m<testTuple.length;m++) {
			instance.setValue(instanceHeader.attribute(m), Double.parseDouble(testTuple[m]));
		}

		return instance;
	}

	/***
	 *
	 * @param fileName
	 * @param l
    * @return
    */
	public static Instances loadDatasetInstances(String fileName, Logger l) {
		try (BufferedReader reader = new BufferedReader(new FileReader(fileName))){
			return loadDatasetInstances(reader, l);
		} catch (IOException e) {
			l.warn("error creading reader for training instances: " + fileName, e);
			return null;
		}
	}

	/***
	 *
	 * @param reader
	 * @param l
     * @return
     */

	
	public static Instances loadDatasetInstances(Reader reader, Logger l) {
		Instances trainingData ;
		try {
			// Read the training data
			trainingData = new Instances(reader);
			// Setting class attribute to last field
			trainingData.setClassIndex(trainingData.numAttributes() - 1);
		} catch (IOException e) {
			l.warn("error loading training instances", e);
			return null;
		}
		return trainingData;
	}
}
