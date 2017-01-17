package in.dream_lab.bm.stream_iot.tasks.io;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableServiceEntity;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
public class AzureTableTask extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// TODO: remove init values after config.properties has been initialized  
	private static String storageConnStr ;
	private static String tableName ;
	private static String partitionKey ;
	private static boolean doneSetup = false;
	private static int startRowKey ;
	private static int endRowKey ;

	private static int useMsgField;
	private static Random rn;
	
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				storageConnStr = p_.getProperty("IO.AZURE_STORAGE_CONN_STR"); //TODO: add to config.property file
				tableName = p_.getProperty("IO.AZURE_TABLE.TABLE_NAME");		// TODO: pass table with TaxiDropoff Entity
				partitionKey = p_.getProperty("IO.AZURE_TABLE.PARTITION_KEY");		// TODO: pass partition with TaxiDropoff Entity
				useMsgField = Integer.parseInt(p_.getProperty("IO.AZURE_TABLE.USE_MSG_FIELD", "0")); // If positive, use that particular field number in the input CSV message as input for count
				startRowKey=Integer.parseInt(p_.getProperty("IO.AZURE_TABLE.START_ROW_KEY"));
				endRowKey=Integer.parseInt(p_.getProperty("IO.AZURE_TABLE.END_ROW_KEY"));
				rn=new Random();
				doneSetup=true;
			}
		}
	}
	
	@Override
	protected Float doTaskLogic(Map map) {
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		CloudTable cloudTbl = connectToAzTable(storageConnStr, tableName, l);
		if(l.isInfoEnabled())
		l.info("Table name is - "+cloudTbl.getName());
		// FIXME: How do you advance the rowkey. Have a start and end for row key as input property?
		String rowKey;
		if(useMsgField>0) {
			rowKey = m.split(",")[useMsgField - 1];
			assert Integer.parseInt(rowKey)>=startRowKey;
			assert Integer.parseInt(rowKey)<=endRowKey;
			if(l.isInfoEnabled())
			l.info("1-row key accesed - "+rowKey);
		}
		else {
			rowKey= String.valueOf(rn.nextInt(endRowKey));
			if(l.isInfoEnabled())
				l.info("2-row key accesed - "+rowKey);
		}
		int result = getAzTableRowByKey(cloudTbl, partitionKey,rowKey, l);
		System.out.println("Row key = "+ rowKey);
//		System.out.println("Result = "+ result);
		return Float.valueOf(result);
	}

	/***
	 *
	 * @param azStorageConnStr
	 * @param tableName
	 * @param l
     * @return
     */
	public static CloudTable connectToAzTable(String azStorageConnStr, String tableName, Logger l) {
		CloudTable cloudTable = null;
		try {
			// Retrieve storage account from connection-string.
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(azStorageConnStr);

			// Create the table client
			CloudTableClient tableClient = storageAccount.createCloudTableClient();

			// Create a cloud table object for the table.
			cloudTable = tableClient.getTableReference(tableName);
		} catch (Exception e) 
		{
			l.warn("Exception in connectToAzTable: "+tableName, e);
		}
		return cloudTable;
	}

	/***
	 *
	 * @param cloudTable
	 * @param partitionKey
	 * @param rowkey
	 * @param l
     * @return
     */
	public static int getAzTableRowByKey(CloudTable cloudTable, String partitionKey, String rowkey, Logger l) {

		try {

			TableOperation retrieveEntity = TableOperation.retrieve(partitionKey, rowkey, TaxiDropoffEntity.class);

			// Submit the operation to the table service and get the specific entity.
			TaxiDropoffEntity entity = cloudTable.execute(retrieveEntity).getResultAsType();

			// Access the entity
			if (entity != null) {
				if (l.isDebugEnabled()) {
					l.debug(entity.getPartitionKey() + "\t " + entity.getRowKey() + "\t"
							+ entity.getDropoff_longitude());
				}

//				System.out.println(entity.getPartitionKey() +
//						"\t " + entity.getRowKey()+"\t"+entity.getDropoff_longitude());

				return entity.getDropoff_longitude().length(); // FIXME: This is specific to the input data in table
			}
		} catch (Exception e) {
			l.warn("Exception in getAzTableRowByKey:"+cloudTable+"; row: "+rowkey, e);
		}
		return -1;
	}

	/***
	 *
	 */

	public static  final class TaxiDropoffEntity extends TableServiceEntity { // FIXME: This is specific to the input data in table

		public TaxiDropoffEntity(){}

	    String Dropoff_datetime;
	    String Dropoff_longitude;

	    public String getDropoff_datetime() {
	        return this.Dropoff_datetime;
	    }

	    public String getDropoff_longitude() {
	        return this.Dropoff_longitude;
	    }


	    public void setDropoff_datetime(String Dropoff_datetime) {
	        this.Dropoff_datetime = Dropoff_datetime;
	    }

	    public void setDropoff_longitude(String Dropoff_longitude) {
	        this.Dropoff_longitude = Dropoff_longitude;
	    }
	}
}
