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


public class AzureTableRangeQueryTaskTAXI extends AbstractTask {

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
		if(l.isInfoEnabled())
		l.info("Table name is - "+cloudTbl.getName());
		// FIXME: How do you advance the rowkey. Have a start and end for row key as input property?
//		String rowKeyStart,rowKeyEnd;
		if(useMsgField>0) {
//			rowKey = m.split(",")[useMsgField - 1];
			rowKeyStart = (String)map.get("ROWKEYSTART");
			rowKeyEnd = (String)map.get("ROWKEYEND");
//			assert Integer.parseInt(rowKeyStart)>=startRowKey;
//			assert Integer.parseInt(rowKeyEnd)<=endRowKey;
			if(l.isInfoEnabled())
			l.info("1-row key accesed till - "+rowKeyEnd);
		}
		else {
			rowKeyStart= String.valueOf(rn.nextInt(endRowKey));
			rowKeyEnd= String.valueOf(rn.nextInt(endRowKey));
			if(l.isInfoEnabled())
				l.info("2-row key accesed - "+rowKeyEnd);
		}
		Iterable<Taxi_Trip> result = getAzTableRangeByKeyTAXI(cloudTbl, partitionKey,rowKeyStart,rowKeyEnd, l);
//		System.out.println("Row key = "+ rowKeyEnd);
//		System.out.println("Result = "+ result);

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
	public static Iterable<Taxi_Trip> getAzTableRangeByKeyTAXI(CloudTable cloudTable, String partitionKey, String rowkeyStart, String rowkeyEnd, Logger l) {

		try {



// filters
			System.out.println("getAzTableRowByKey-"+rowkeyStart+","+rowkeyEnd);

			// Create a filter condition where the partition key is "Smith".
			String partitionFilter = TableQuery.generateFilterCondition(
					"PartitionKey",
					TableQuery.QueryComparisons.EQUAL,
					"taxi_range");

			String filter2 = TableQuery.generateFilterCondition("RangeTs", TableQuery.QueryComparisons.GREATER_THAN_OR_EQUAL, Long.parseLong(rowkeyStart)); // recordStart i.e.: "123"
			String filter3 = TableQuery.generateFilterCondition("RangeTs", TableQuery.QueryComparisons.LESS_THAN, Long.parseLong(rowkeyEnd)); // recordEnd i.e.: "567"

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

			TableQuery<Taxi_Trip> rangeQuery =
					TableQuery.from(Taxi_Trip.class)
							.where(combinedFilter);

			Iterable<Taxi_Trip> queryRes = cloudTable.execute(rangeQuery);

//			// Loop through the results, displaying information about the entity
//			for (Taxi_Trip entity : queryRes) {
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

//	public static  final class TaxiDropoffEntity extends TableServiceEntity { // FIXME: This is specific to the input data in table
//
//		public TaxiDropoffEntity(){}
//
//		String Dropoff_datetime;
//	    String Dropoff_longitude;
//		String Fare_amount;
//		String Airquality_raw;
//
//
//	    public String getDropoff_datetime() {
//	        return this.Dropoff_datetime;
//	    }
//	    public String getDropoff_longitude() {
//	        return this.Dropoff_longitude;
//	    }
//		public String getFare_amount() {
//			return this.Fare_amount;
//		}
//		public String getAirquality_raw() {
//			return this.Airquality_raw;
//		}
//
//	    public void setDropoff_datetime(String Dropoff_datetime) {
//	        this.Dropoff_datetime = Dropoff_datetime;
//	    }
//	    public void setDropoff_longitude(String Dropoff_longitude) {
//	        this.Dropoff_longitude = Dropoff_longitude;
//	    }
//		public void setFare_amount(String Fare_amount) {
//			this.Fare_amount = Fare_amount;
//		}
//		public void setAirquality_raw(String Airquality_raw) {
//			this.Airquality_raw = Airquality_raw;
//		}
//
//		String Temperature;
//		public String getTemperature() {
//			return this.Temperature;
//		}
//		public void setTemperature(String Temperature) {
//			this.Temperature = Temperature;
//		}
//	}

	public static  final class Taxi_Trip extends TableServiceEntity
	{
		private String taxi_identifier,hack_license, pickup_datetime,drop_datetime;
		private long rangeTs;

		public long getRangeTs() {
			return rangeTs;
		}

		public void setRangeTs(long rangeTs) {
			this.rangeTs = rangeTs;
		}

		public String getDrop_datetime() {
			return drop_datetime;
		}

		public void setDrop_datetime(String drop_datetime) {
			this.drop_datetime = drop_datetime;
		}
		private String trip_time_in_secs,trip_distance;
		private String pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type;
		private String fare_amount,surcharge,mta_tax,tip_amount,tolls_amount, total_amount;

		public String getTaxi_identifier() {
			return taxi_identifier;
		}

		public void setTaxi_identifier(String taxi_identifier) {
			this.taxi_identifier = taxi_identifier;
		}

		public String getHack_license() {
			return hack_license;
		}

		public void setHack_license(String hack_license) {
			this.hack_license = hack_license;
		}

		public String getPickup_datetime() {
			return pickup_datetime;
		}

		public void setPickup_datetime(String l) {
			this.pickup_datetime = l;
		}

		public String getTrip_time_in_secs() {
			return trip_time_in_secs;
		}

		public void setTrip_time_in_secs(String trip_time_in_secs) {
			this.trip_time_in_secs = trip_time_in_secs;
		}

		public String getTrip_distance() {
			return trip_distance;
		}

		public void setTrip_distance(String trip_distance) {
			this.trip_distance = trip_distance;
		}

		public String getPickup_longitude() {
			return pickup_longitude;
		}
		public void setPickup_longitude(String pickup_longitude) {
			this.pickup_longitude = pickup_longitude;
		}

		public String getPickup_latitude() {
			return pickup_latitude;
		}

		public void setPickup_latitude(String pickup_latitude) {
			this.pickup_latitude = pickup_latitude;
		}

		public String getDropoff_longitude() {
			return dropoff_longitude;
		}

		public void setDropoff_longitude(String dropoff_longitude) {
			this.dropoff_longitude = dropoff_longitude;
		}

		public String getDropoff_latitude() {
			return dropoff_latitude;
		}

		public void setDropoff_latitude(String dropoff_latitude) {
			this.dropoff_latitude = dropoff_latitude;
		}

		public String getPayment_type() {
			return payment_type;
		}
		public void setPayment_type(String payment_type) {
			this.payment_type = payment_type;
		}

		public String getFare_amount() {
			return fare_amount;
		}
		public void setFare_amount(String fare_amount) {
			this.fare_amount = fare_amount;
		}

		public String getSurcharge() {
			return surcharge;
		}

		public void setSurcharge(String surcharge) {
			this.surcharge = surcharge;
		}

		public String getMta_tax() {
			return mta_tax;
		}

		public void setMta_tax(String mta_tax) {
			this.mta_tax = mta_tax;
		}

		public String getTip_amount() {
			return tip_amount;
		}

		public void setTip_amount(String tip_amount) {
			this.tip_amount = tip_amount;
		}

		public String getTolls_amount() {
			return tolls_amount;
		}

		public void setTolls_amount(String tolls_amount) {
			this.tolls_amount = tolls_amount;
		}

		public String getTotal_amount() {
			return total_amount;
		}

		public void setTotal_amount(String total_amount) {
			this.total_amount = total_amount;
		}

		public static  Taxi_Trip parseString(String s,long counter)
		{
			Taxi_Trip taxiObj = new Taxi_Trip();
			String fields[] = s.split(",");
//			Random r = new Random();
			taxiObj.rowKey = fields[0]+"-" +fields[1]+"-"+fields[2] ;
			taxiObj.partitionKey = "taxi_range";
			DateTime d = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(fields[3]);
			taxiObj.setRangeTs(d.getMillis());
			taxiObj.setTaxi_identifier(fields[0]);
			taxiObj.setHack_license(fields[1]);
			taxiObj.setPickup_datetime(fields[2]);
			taxiObj.setDrop_datetime(fields[3]);
			taxiObj.setTrip_time_in_secs(fields[4]);
			taxiObj.setTrip_distance(fields[5]);
			taxiObj.setPickup_longitude(fields[6]);
			taxiObj.setPickup_latitude(fields[7]);
			taxiObj.setDropoff_longitude(fields[8]);
			taxiObj.setDropoff_latitude(fields[9]);
			taxiObj.setPayment_type(fields[10]);
			taxiObj.setFare_amount(fields[11]);
			taxiObj.setSurcharge(fields[12]);
			taxiObj.setMta_tax(fields[13]);
			taxiObj.setTip_amount(fields[14]);
			taxiObj.setTolls_amount(fields[15]);
			taxiObj.setTotal_amount(fields[16]);

			return taxiObj;
		}
	}


}
