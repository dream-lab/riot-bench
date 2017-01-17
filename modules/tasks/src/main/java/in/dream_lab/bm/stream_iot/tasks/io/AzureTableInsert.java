package in.dream_lab.bm.stream_iot.tasks.io;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableServiceEntity;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableTask.TaxiDropoffEntity;
/**
 * @author shilpa
 *
 */
public class AzureTableInsert extends AbstractTask
{	
	private static final Object SETUP_LOCK = new Object();
	private static String storageConnStr ;
	private static String tableName ;
	private static boolean doneSetup = false;
	private static int useMsgField;
	private static String entity;
	private CloudTable table ;
	private String sampleData;  
	@Override
	public void setup(Logger l_, Properties p_) 
	{
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) 
		{
			if(!doneSetup)
			{ 
				storageConnStr = p_.getProperty("IO.AZURE_STORAGE_CONN_STR"); 
				tableName = p_.getProperty("IO.AZURE_TABLE.TABLE_NAME");		
				useMsgField = Integer.parseInt(p_.getProperty("IO.AZURE_TABLE.USE_MSG_FIELD", "0")); 
				entity = p_.getProperty("IO.AZURE_TABLE.WRITE_ENTITY");
				doneSetup=true;
			}
			table = connectToAzTable(storageConnStr,tableName ,l );
			/* added for use in benchmarking task with sample data */
			sampleData = "024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14 03:57:00,1358117580000,-1,0,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,11.00,0.50,13.00,4.80,70.30,uber,sam,Houston";
		}
	}

	@Override
	protected Float doTaskLogic(Map map) 
	{
		try 
		{
			String m = (String)map.get(AbstractTask.DEFAULT_KEY);
			TaxiTrip taxiEntity = new TaxiTrip();
			if (useMsgField == -1)
				taxiEntity.parseString(sampleData);
			else
				taxiEntity.parseString(m);
			
			// Create an operation to add the new entity to the taxi data table.
//			TableOperation operation = TableOperation.insertOrReplace(taxiEntity);
			
			TableBatchOperation batchOperation = new TableBatchOperation();
			batchOperation.insertOrReplace(taxiEntity);
			// Submit the operation to the table service.
			table.execute(batchOperation);
			System.out.println("Execute succ");
		}
			catch (StorageException e) 
			{
				e.printStackTrace();
			}
			return 0.1f;
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
	
	
	public static  final class TaxiTrip extends TableServiceEntity
	{
		private String taxi_identifier,hack_license, pickup_datetime,drop_datetime; 
		public String getDrop_datetime() {
			return drop_datetime;
		}

		public void setDrop_datetime(String drop_datetime) {
			this.drop_datetime = drop_datetime;
		}
		private double trip_time_in_secs,trip_distance;
		private String pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type;
		private double fare_amount,surcharge,mta_tax,tip_amount,tolls_amount, total_amount;
		private String company, driver, city;
		public String getCompany() {
			return company;
		}

		public void setCompany(String company) {
			this.company = company;
		}

		public String getDriver() {
			return driver;
		}

		public void setDriver(String driver) {
			this.driver = driver;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

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
		
		public double getTrip_time_in_secs() {
			return trip_time_in_secs;
		}

		public void setTrip_time_in_secs(double trip_time_in_secs) {
			this.trip_time_in_secs = trip_time_in_secs;
		}

		public double getTrip_distance() {
			return trip_distance;
		}

		public void setTrip_distance(double trip_distance) {
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
		
		public double getFare_amount() {
			return fare_amount;
		}
		public void setFare_amount(double fare_amount) {
			this.fare_amount = fare_amount;
		}

		public double getSurcharge() {
			return surcharge;
		}

		public void setSurcharge(double surcharge) {
			this.surcharge = surcharge;
		}

		public double getMta_tax() {
			return mta_tax;
		}

		public void setMta_tax(double mta_tax) {
			this.mta_tax = mta_tax;
		}

		public double getTip_amount() {
			return tip_amount;
		}

		public void setTip_amount(double tip_amount) {
			this.tip_amount = tip_amount;
		}

		public double getTolls_amount() {
			return tolls_amount;
		}

		public void setTolls_amount(double tolls_amount) {
			this.tolls_amount = tolls_amount;
		}

		public double getTotal_amount() {
			return total_amount;
		}

		public void setTotal_amount(double total_amount) {
			this.total_amount = total_amount;
		}
		
		public void parseString(String s)
		{
			String fields[] = s.split(",");
			this.rowKey = fields[0];
			this.partitionKey = fields[1];
			this.setTaxi_identifier(fields[0]);
			this.setHack_license(fields[1]);
			this.setPickup_datetime(fields[2]);
			this.setDrop_datetime(fields[3]);
			this.setTrip_time_in_secs(Double.parseDouble(fields[4]));
			this.setTrip_distance(Double.parseDouble(fields[5]));
			this.setPickup_longitude(fields[6]);
			this.setPickup_latitude(fields[7]);
			this.setDropoff_longitude(fields[8]);
			this.setDropoff_latitude(fields[9]);
			this.setPayment_type(fields[10]);
			this.setFare_amount(Double.parseDouble(fields[11]));
			this.setSurcharge(Double.parseDouble(fields[12]));
			this.setMta_tax(Double.parseDouble(fields[13]));
			this.setTip_amount(Double.parseDouble(fields[14]));
			this.setTolls_amount(Double.parseDouble(fields[15]));
			this.setTotal_amount(Double.parseDouble(fields[16]));
			this.setCompany(fields[17]);
			this.setDriver(fields[18]);
			this.setCity(fields[19]);
		}
	}


	
}
