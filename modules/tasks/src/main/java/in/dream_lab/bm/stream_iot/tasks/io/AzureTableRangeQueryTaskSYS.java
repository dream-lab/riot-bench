package in.dream_lab.bm.stream_iot.tasks.io;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.*;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;


public class AzureTableRangeQueryTaskSYS extends AbstractTask {

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
		Iterable<SYS_City> result = getAzTableRangeByKeySYS(cloudTbl, partitionKey,rowKeyStart,rowKeyEnd, l);
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
	public static Iterable<SYS_City> getAzTableRangeByKeySYS(CloudTable cloudTable, String partitionKey, String rowkeyStart, String rowkeyEnd, Logger l) {

		try {



// filters
			System.out.println("getAzTableRowByKey-"+rowkeyStart+","+rowkeyEnd);

			// Create a filter condition where the partition key is "Smith".
			String partitionFilter = TableQuery.generateFilterCondition(
					"PartitionKey",
					TableQuery.QueryComparisons.EQUAL,
					"sys_range");

			String filter2 = TableQuery.generateFilterCondition("RangeTs", TableQuery.QueryComparisons.GREATER_THAN_OR_EQUAL, Long.parseLong(rowkeyStart)); // recordStart i.e.: "123"
			String filter3 = TableQuery.generateFilterCondition("RangeTs", TableQuery.QueryComparisons.LESS_THAN, Long.parseLong(rowkeyEnd)); // recordEnd i.e.: "567"

			String filterRange = TableQuery.combineFilters(filter2, TableQuery.Operators.AND, filter3);

			// Combine the two conditions into a filter expression.
			String combinedFilter = TableQuery.combineFilters(partitionFilter,
					TableQuery.Operators.AND, filterRange);

			TableQuery<SYS_City> rangeQuery =
					TableQuery.from(SYS_City.class)
							.where(combinedFilter);

			Iterable<SYS_City> queryRes = cloudTable.execute(rangeQuery);

			// Loop through the results, displaying information about the entity
//			for (SYS_City entity : queryRes) {
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

	public static  final class TaxiDropoffEntity extends TableServiceEntity { // FIXME: This is specific to the input data in table

		public TaxiDropoffEntity(){}

		String Dropoff_datetime;
	    String Dropoff_longitude;
		String Fare_amount;
		String Airquality_raw;


	    public String getDropoff_datetime() {
	        return this.Dropoff_datetime;
	    }
	    public String getDropoff_longitude() {
	        return this.Dropoff_longitude;
	    }
		public String getFare_amount() {
			return this.Fare_amount;
		}
		public String getAirquality_raw() {
			return this.Airquality_raw;
		}

	    public void setDropoff_datetime(String Dropoff_datetime) {
	        this.Dropoff_datetime = Dropoff_datetime;
	    }
	    public void setDropoff_longitude(String Dropoff_longitude) {
	        this.Dropoff_longitude = Dropoff_longitude;
	    }
		public void setFare_amount(String Fare_amount) {
			this.Fare_amount = Fare_amount;
		}
		public void setAirquality_raw(String Airquality_raw) {
			this.Airquality_raw = Airquality_raw;
		}

		String Temperature;
		public String getTemperature() {
			return this.Temperature;
		}
		public void setTemperature(String Temperature) {
			this.Temperature = Temperature;
		}
	}


	public static  final class SYS_City extends TableServiceEntity
	{
		private String ts,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw,location,type;
		private long rangeKey;
		public long getRangeKey() {
			return rangeKey;
		}
		public void setRangeKey(long rangeKey) {
			this.rangeKey = rangeKey;
		}
		public String getTs() {
			return ts;
		}
		public void setTs(String timestamp) {
			this.ts = timestamp;
		}
		public String getSource() {
			return source;
		}
		public void setSource(String source) {
			this.source = source;
		}
		public String getLongitude() {
			return longitude;
		}
		public void setLongitude(String longitude) {
			this.longitude = longitude;
		}
		public String getLatitude() {
			return latitude;
		}
		public void setLatitude(String latitude) {
			this.latitude = latitude;
		}
		public String getTemperature() {
			return temperature;
		}
		public void setTemperature(String temperature) {
			this.temperature = temperature;
		}
		public String getHumidity() {
			return humidity;
		}
		public void setHumidity(String humidity) {
			this.humidity = humidity;
		}
		public String getLight() {
			return light;
		}
		public void setLight(String light) {
			this.light = light;
		}
		public String getDust() {
			return dust;
		}
		public void setDust(String dust) {
			this.dust = dust;
		}
		public String getAirquality_raw() {
			return airquality_raw;
		}
		public void setAirquality_raw(String airquality_raw) {
			this.airquality_raw = airquality_raw;
		}
		public String getLocation() {
			return location;
		}
		public void setLocation(String location) {
			this.location = location;
		}
		private String getType() {
			return type;
		}
		private void setType(String type) {
			this.type = type;
		}

		public static  SYS_City parseString(String s, long counter)
		{
			try
			{
				SYS_City obj = new SYS_City();
				String fields[] = s.split(",");
//			System.out.println("String s "+ s);
				Random r = new Random();
				obj.rowKey = fields[0]+"-" +fields[1];
				obj.partitionKey = "sys_range";
				obj.setRangeKey(counter);
				obj.setTs(fields[0]);
				obj.setSource(fields[1]);
				obj.setLongitude(fields[2]);
				obj.setLatitude(fields[3]);
				obj.setTemperature(fields[4]);
				obj.setHumidity(fields[5]);
				obj.setLight(fields[6]);
				obj.setDust(fields[7]);
				obj.setAirquality_raw(fields[8]);
//			obj.setLocation(fields[9]);
//			obj.setType(fields[10]);
				return obj;
			}
			catch(Exception e )
			{
				System.out.println("Exception in parse "+ e.getMessage());
			}
			return null;
		}

	}


}
