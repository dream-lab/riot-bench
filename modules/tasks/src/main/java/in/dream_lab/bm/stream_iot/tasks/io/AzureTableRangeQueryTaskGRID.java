package in.dream_lab.bm.stream_iot.tasks.io;

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableServiceEntity;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.Random;


public class AzureTableRangeQueryTaskGRID extends AbstractTask {

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
		String rowKeyStart,rowKeyEnd;
		CloudTable cloudTbl = connectToAzTable(storageConnStr, tableName, l);
		l.warn("Table name is - "+cloudTbl.getName());
		// FIXME: How do you advance the rowkey. Have a start and end for row key as input property?
//		String rowKeyStart,rowKeyEnd;
		if(useMsgField>0) {
//			rowKey = m.split(",")[useMsgField - 1];
			rowKeyStart = (String)map.get("ROWKEYSTART");
			rowKeyEnd = (String)map.get("ROWKEYEND");
			assert Integer.parseInt(rowKeyStart)>=startRowKey;
			assert Integer.parseInt(rowKeyEnd)<=endRowKey;
			if(l.isInfoEnabled())
			l.info("1-row key accesed till - "+rowKeyEnd);
		}
		else {
			rowKeyStart= String.valueOf(rn.nextInt(endRowKey));
			rowKeyEnd= String.valueOf(rn.nextInt(endRowKey));
			if(l.isInfoEnabled())
				l.info("2-row key accesed - "+rowKeyEnd);
		}
		Iterable<GRID_data> result = getAzTableRangeByKeyTAXI(cloudTbl, partitionKey,rowKeyStart,rowKeyEnd, l);
		System.out.println("Row key = "+ rowKeyEnd);
		System.out.println("Result = "+ result);

		super.setLastResult(result);

		return Float.valueOf(Lists.newArrayList(result).size());  // may need updation
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
//	 * @param rowkey
	 * @param l
     * @return
     */
	public static Iterable<GRID_data> getAzTableRangeByKeyTAXI(CloudTable cloudTable, String partitionKey, String rowkeyStart, String rowkeyEnd, Logger l) {

		try {



// filters
			System.out.println("getAzTableRowByKey-"+rowkeyStart+","+rowkeyEnd);

			// Create a filter condition where the partition key is "Smith".
			String partitionFilter = TableQuery.generateFilterCondition(
					"PartitionKey",
					TableQuery.QueryComparisons.EQUAL,
					"sys_range");

			String filter2 = TableQuery.generateFilterCondition("rangeTs", TableQuery.QueryComparisons.GREATER_THAN_OR_EQUAL, Long.parseLong(rowkeyStart)); // recordStart i.e.: "123"
			String filter3 = TableQuery.generateFilterCondition("rangeTs", TableQuery.QueryComparisons.LESS_THAN, Long.parseLong(rowkeyEnd)); // recordEnd i.e.: "567"

			String filterRange = TableQuery.combineFilters(filter2, TableQuery.Operators.AND, filter3);

			// Combine the two conditions into a filter expression.
			String combinedFilter = TableQuery.combineFilters(partitionFilter,
					TableQuery.Operators.AND, filterRange);


//			TableQuery<TaxiDropoffEntity> rangeQuery =
//					TableQuery.from(TaxiDropoffEntity.class)
//							.where(combinedFilter);

//					Iterable<TaxiDropoffEntity> queryRes = cloudTable.execute(rangeQuery);
//			// Loop through the results, displaying information about the entity
//			for (TaxiDropoffEntity entity : queryRes) {
//				System.out.println(entity.getPartitionKey() +
//						" " + entity.getRowKey() +
//						"\t" + entity.Temperature
//						);
//			}

			TableQuery<GRID_data> rangeQuery =
					TableQuery.from(GRID_data.class)
							.where(combinedFilter);

			Iterable<GRID_data> queryRes = cloudTable.execute(rangeQuery);

//			// Loop through the results, displaying information about the entity
//			for (GRID_data entity : queryRes) {
//				System.out.println(entity.getPartitionKey() +
//						" " + entity.getRangeKey() +
//						"\t" + entity.getAirquality_raw()
//				);
//			}


			return queryRes;
		} catch (Exception e) {
			l.warn("Exception in getAzTableRowByKey:"+cloudTable+"; rowkeyEnd: "+rowkeyEnd, e);
		}
		return null;
	}

	/***
	 *
	 */

	public static final class GRID_data extends TableServiceEntity
	{
		private String meterid,ts,energyconsumed;
		private long rangeTs;

		public long getRangeTs() {
			return rangeTs;
		}
		public void setRangeTs(long rangeTs) {
			this.rangeTs = rangeTs;
		}
		public String getTs() {
			return ts;
		}
		public void setTs(String ts) {
			this.ts = ts;
		}

		public String getMeterid() {
			return meterid;
		}

		public void setMeterid(String meterid) {
			this.meterid = meterid;
		}

		public String getEnergyconsumed() {
			return energyconsumed;
		}

		public void setEnergyconsumed(String energyconsumed) {
			this.energyconsumed = energyconsumed;
		}

		public static  GRID_data parseString(String s, long counter)
		{
			GRID_data gridData = new GRID_data();
			String fields[] = s.split(",");
			gridData.rowKey = fields[0]+"-"+fields[1];
			gridData.partitionKey = "grid_range";
			gridData.setMeterid(fields[0]);
			gridData.setTs(fields[1]);
			gridData.setEnergyconsumed(fields[2]);
			gridData.setRangeTs(Long.parseLong(fields[1]));
			return gridData;
		}
	}


}
