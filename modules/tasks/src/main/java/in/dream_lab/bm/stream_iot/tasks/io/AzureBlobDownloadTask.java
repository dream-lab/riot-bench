package in.dream_lab.bm.stream_iot.tasks.io;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Downloads a blob from Azure cloud to local memory
 * 
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shukla, simmhan
 *
 */
public class AzureBlobDownloadTask extends AbstractTask<String,byte[]> {

	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int useMsgField;

	private static String storageConnStr;
	private static String containerName;
	private static String[] fileNames;

	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				useMsgField = Integer.parseInt(p_.getProperty("IO.AZURE_BLOB_DOWNLOAD.USE_MSG_FIELD", "0"));


				storageConnStr = p_.getProperty("IO.AZURE_STORAGE_CONN_STR");
				containerName = p_.getProperty("IO.AZURE_BLOB.CONTAINER_NAME");
				String csvFileNames = p_.getProperty("IO.AZURE_BLOB_DOWNLOAD.FILE_NAMES"); //multiple CSV files names
				assert csvFileNames != null;
				fileNames = csvFileNames.split(",");
				doneSetup=true;
			}
		}
	}
	
	@Override
	protected Float doTaskLogic(Map<String, String> map) {
		String m = map.get(AbstractTask.DEFAULT_KEY);
		// get file index to be downloaded from message or at random
		int fileindex;
		if(useMsgField > 0){
			fileindex = Integer.parseInt(m.split(",")[useMsgField-1]) % fileNames.length; 
		}
		else {
			fileindex = ThreadLocalRandom.current().nextInt(fileNames.length);
		}

		String fileName = fileNames[fileindex];
		if (useMsgField==0){
			fileName=m; // getting file name from mqttpublish
		}

//		fileName = "peakRateBarplot.pdf";
		CloudBlob cloudBlob = connectToAzBlob(storageConnStr, containerName, fileName, l);
		assert cloudBlob != null;
		int result = getAzBlob(cloudBlob, l);
		return Float.valueOf(result);
	}


	/***
	 *
	 * @param azStorageConnStr
	 * @param containerName
	 * @param fileName
	 * @param l
     * @return
     */
	static CloudBlob connectToAzBlob(String azStorageConnStr, String containerName, String fileName, Logger l) {
		try {
			// Retrieve storage account from connection-string.
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(azStorageConnStr);

			// Create the blob client.
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			// Retrieve reference to a previously created container.
			CloudBlobContainer container = blobClient.getContainerReference(containerName);

			ListBlobItem blobItem =container.getBlockBlobReference(fileName);

			CloudBlob blob = (CloudBlob) blobItem;
//			blob.download(new FileOutputStream("/home/shilpa/Desktop/" + blob.getName()));
//			System.out.println("File saved");
			return blob;
		} catch (Exception e) {
			l.warn("Exception in connectToAzBlob: "+containerName+"/"+fileName, e);
		}

		return null;
	}

	/***
	 *
	 * @param blob
	 * @param l
     * @return
     */
	public  int getAzBlob(CloudBlob blob, Logger l) {
		try {
			blob.downloadAttributes();
			int blobSize = (int)(blob.getProperties().getLength());

			//preallocate memory of adequate size
			 ByteArrayOutputStream output = new ByteArrayOutputStream(blobSize);
             
			 
			blob.download(output);
			if(l.isInfoEnabled())
				l.info("ByteArrayOutputStream size -"+output.size());

			super.setLastResult(output.toByteArray());

			return output.size();
		} catch (Exception e) {
			l.warn("Exception in getAzBlob: "+blob, e);
		}
		return -1;		
	}
}
