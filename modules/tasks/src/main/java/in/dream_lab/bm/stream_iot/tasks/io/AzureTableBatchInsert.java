package in.dream_lab.bm.stream_iot.tasks.io;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableServiceEntity;


public class AzureTableBatchInsert extends AbstractTask<String,Float>
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
		}
	}

	@Override
	public Float doTaskLogic(Map map) 
	{
		String tuple;
		 // Define a batch operation.
	    TableBatchOperation batchOperation = new TableBatchOperation();
		try 
		{
			for(int i = 0; i < map.size(); i++)
			{
				tuple = (String)map.get(String.valueOf(i));
				//For Taxi dataset 
				TaxiTrip obj = TaxiTrip.parseString(tuple);
				
				/*For FIT dataset 
				FITdata obj = FITdata.parseString(tuple);
				*/
				batchOperation.insert(obj);			
			 }
			ArrayList a = table.execute(batchOperation);
			return (float)a.size();
		}
			catch (Exception e) 
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
			l.warn("$$$$$$$$$$$$$Exception in connectToAzTable: "+tableName, e);
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
		private String trip_time_in_secs,trip_distance;
		private String pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type;
		private String fare_amount,surcharge,mta_tax,tip_amount,tolls_amount, total_amount;
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
		
		public static  TaxiTrip parseString(String s)
		{
			TaxiTrip taxiObj = new TaxiTrip();
			String fields[] = s.split(",");
			Random r = new Random(); 
			taxiObj.rowKey = fields[0]+"-" +fields[1]+"-"+fields[2] + r.nextInt(100);
			taxiObj.partitionKey = "partition";
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
			taxiObj.setCompany(fields[17]);
			taxiObj.setDriver(fields[18]);
			taxiObj.setCity(fields[19]);
			return taxiObj;
		}
	}
	
	public static  final class SYSCity extends TableServiceEntity
	{
		private String ts,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw,location,type;
		
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
		
		public static  SYSCity parseString(String s)
		{
			SYSCity obj = new SYSCity();
			String fields[] = s.split(",");
			Random r = new Random(); 
			obj.rowKey = fields[0]+"-" +fields[1]+"-"+r.nextInt(100);
			obj.partitionKey = "partition";
			obj.setTs(fields[0]);
			obj.setSource(fields[1]);
			obj.setLongitude(fields[2]);
			obj.setLatitude(fields[3]);
			obj.setTemperature(fields[4]);
			obj.setHumidity(fields[5]);
			obj.setLight(fields[6]);
			obj.setDust(fields[7]);
			obj.setAirquality_raw(fields[8]);
			obj.setLocation(fields[9]);
			obj.setType(fields[10]);			
			return obj;
		}

	}

	public static final class FITdata extends TableServiceEntity
	{
		private String subjectId,acc_chest_x,acc_chest_y,acc_chest_z,
		               ecg_lead_1,ecg_lead_2,acc_ankle_x,acc_ankle_y,acc_ankle_z,
		               gyro_ankle_x,gyro_ankle_y,gyro_ankle_z,
		               magnetometer_ankle_x,magnetometer_ankle_y,magnetometer_ankle_z,
		               acc_arm_x,acc_arm_y,acc_arm_z,
		               gyro_arm_x,gyro_arm_y,gyro_arm_z,
		               magnetometer_arm_x,magnetometer_arm_y,magnetometer_arm_z,
		               label, age, gender;
		private long ts;
		public static  FITdata parseString(String s)
		{
			FITdata obj = new FITdata();
			String fields[] = s.split(",");
			Random r = new Random(); 
			obj.rowKey = fields[0]+"-" +fields[1] +r.nextInt(100);
			obj.partitionKey = "partition";
			obj.setSubjectId(fields[0]);
			obj.setTs(Long.parseLong(fields[1]));
			obj.setAcc_chest_x(fields[2]);
			obj.setAcc_chest_y(fields[3]);
			obj.setAcc_chest_z(fields[4]);
			obj.setEcg_lead_1(fields[5]);
			obj.setEcg_lead_1(fields[6]);
			obj.setAcc_ankle_x(fields[7]);
			obj.setAcc_ankle_y(fields[8]);
			obj.setAcc_ankle_z(fields[9]);
			obj.setGyro_ankle_x(fields[10]);
			obj.setGyro_ankle_y(fields[11]);
			obj.setGyro_ankle_z(fields[12]);
			obj.setMagnetometer_ankle_x(fields[13]);
			obj.setMagnetometer_ankle_y(fields[14]);
			obj.setMagnetometer_ankle_z(fields[15]);
			obj.setAcc_arm_x(fields[16]);
			obj.setAcc_arm_y(fields[17]);
			obj.setAcc_arm_z(fields[18]);
			obj.setAcc_arm_z(fields[19]);
			obj.setGyro_arm_x(fields[20]);
			obj.setGyro_arm_y(fields[21]);
			obj.setGyro_arm_z(fields[22]);
			obj.setMagnetometer_arm_x(fields[23]);
			obj.setMagnetometer_arm_y(fields[24]);
			obj.setMagnetometer_arm_z(fields[25]);
			obj.setLabel(fields[26]);
			obj.setAge(fields[27]);
			obj.setGender(fields[28]);
			return obj;
		}
		
		
		public String getAge() {
			return age;
		}


		public void setAge(String age) {
			this.age = age;
		}


		public String getGender() {
			return gender;
		}


		public void setGender(String gender) {
			this.gender = gender;
		}


		public String getSubjectId() {
			return subjectId;
		}

		public void setSubjectId(String subjectId) {
			this.subjectId = subjectId;
		}

		public long getTs() {
			return ts;
		}

		public void setTs(long timestamp) {
			this.ts = timestamp;
		}

		public String getAcc_chest_x() {
			return acc_chest_x;
		}

		public void setAcc_chest_x(String acc_chest_x) {
			this.acc_chest_x = acc_chest_x;
		}

		public String getAcc_chest_y() {
			return acc_chest_y;
		}

		public void setAcc_chest_y(String acc_chest_y) {
			this.acc_chest_y = acc_chest_y;
		}

		public String getAcc_chest_z() {
			return acc_chest_z;
		}

		public void setAcc_chest_z(String acc_chest_z) {
			this.acc_chest_z = acc_chest_z;
		}

		public String getEcg_lead_1() {
			return ecg_lead_1;
		}

		public void setEcg_lead_1(String ecg_lead_1) {
			this.ecg_lead_1 = ecg_lead_1;
		}

		public String getEcg_lead_2() {
			return ecg_lead_2;
		}

		public void setEcg_lead_2(String ecg_lead_2) {
			this.ecg_lead_2 = ecg_lead_2;
		}

		public String getAcc_ankle_x() {
			return acc_ankle_x;
		}

		public void setAcc_ankle_x(String acc_ankle_x) {
			this.acc_ankle_x = acc_ankle_x;
		}

		public String getAcc_ankle_y() {
			return acc_ankle_y;
		}

		public void setAcc_ankle_y(String acc_ankle_y) {
			this.acc_ankle_y = acc_ankle_y;
		}

		public String getAcc_ankle_z() {
			return acc_ankle_z;
		}

		public void setAcc_ankle_z(String acc_ankle_z) {
			this.acc_ankle_z = acc_ankle_z;
		}

		public String getGyro_ankle_x() {
			return gyro_ankle_x;
		}

		public void setGyro_ankle_x(String gyro_ankle_x) {
			this.gyro_ankle_x = gyro_ankle_x;
		}

		public String getGyro_ankle_y() {
			return gyro_ankle_y;
		}

		public void setGyro_ankle_y(String gyro_ankle_y) {
			this.gyro_ankle_y = gyro_ankle_y;
		}

		public String getGyro_ankle_z() {
			return gyro_ankle_z;
		}

		public void setGyro_ankle_z(String gyro_ankle_z) {
			this.gyro_ankle_z = gyro_ankle_z;
		}

		public String getMagnetometer_ankle_x() {
			return magnetometer_ankle_x;
		}

		public void setMagnetometer_ankle_x(String magnetometer_ankle_x) {
			this.magnetometer_ankle_x = magnetometer_ankle_x;
		}

		public String getMagnetometer_ankle_y() {
			return magnetometer_ankle_y;
		}

		public void setMagnetometer_ankle_y(String magnetometer_ankle_y) {
			this.magnetometer_ankle_y = magnetometer_ankle_y;
		}

		public String getMagnetometer_ankle_z() {
			return magnetometer_ankle_z;
		}

		public void setMagnetometer_ankle_z(String magnetometer_ankle_z) {
			this.magnetometer_ankle_z = magnetometer_ankle_z;
		}

		public String getAcc_arm_x() {
			return acc_arm_x;
		}

		public void setAcc_arm_x(String acc_arm_x) {
			this.acc_arm_x = acc_arm_x;
		}

		public String getAcc_arm_y() {
			return acc_arm_y;
		}

		public void setAcc_arm_y(String acc_arm_y) {
			this.acc_arm_y = acc_arm_y;
		}

		public String getAcc_arm_z() {
			return acc_arm_z;
		}

		public void setAcc_arm_z(String acc_arm_z) {
			this.acc_arm_z = acc_arm_z;
		}

		public String getGyro_arm_x() {
			return gyro_arm_x;
		}

		public void setGyro_arm_x(String gyro_arm_x) {
			this.gyro_arm_x = gyro_arm_x;
		}

		public String getGyro_arm_y() {
			return gyro_arm_y;
		}

		public void setGyro_arm_y(String gyro_arm_y) {
			this.gyro_arm_y = gyro_arm_y;
		}

		public String getGyro_arm_z() {
			return gyro_arm_z;
		}

		public void setGyro_arm_z(String gyro_arm_z) {
			this.gyro_arm_z = gyro_arm_z;
		}

		public String getMagnetometer_arm_x() {
			return magnetometer_arm_x;
		}

		public void setMagnetometer_arm_x(String magnetometer_arm_x) {
			this.magnetometer_arm_x = magnetometer_arm_x;
		}

		public String getMagnetometer_arm_y() {
			return magnetometer_arm_y;
		}

		public void setMagnetometer_arm_y(String magnetometer_arm_y) {
			this.magnetometer_arm_y = magnetometer_arm_y;
		}

		public String getMagnetometer_arm_z() {
			return magnetometer_arm_z;
		}

		public void setMagnetometer_arm_z(String magnetometer_arm_z) {
			this.magnetometer_arm_z = magnetometer_arm_z;
		}

		public String getLabel() {
			return label;
		}

		public void setLabel(String label) {
			this.label = label;
		}
		
	}

	public static final class GRIDdata extends TableServiceEntity
	{
		private String meterid,ts,energyconsumed, code,tariff,stimulus,sme;
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

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}

		public String getTariff() {
			return tariff;
		}

		public void setTariff(String tariff) {
			this.tariff = tariff;
		}

		public String getStimulus() {
			return stimulus;
		}

		public void setStimulus(String stimulus) {
			this.stimulus = stimulus;
		}

		public String getSme() {
			return sme;
		}

		public void setSme(String sme) {
			this.sme = sme;
		}

		public static  GRIDdata parseString(String s)
		{
			GRIDdata gridData = new GRIDdata();
			String fields[] = s.split(",");
			Random r = new Random(); 
			gridData.rowKey = fields[0]+"-"+fields[1];
			gridData.partitionKey = "partition2";
			gridData.setMeterid(fields[0]);
			gridData.setTs(fields[1]);
			gridData.setEnergyconsumed(fields[2]);
			gridData.setCode(fields[3]);
			gridData.setTariff(fields[4]);
			gridData.setStimulus(fields[5]);
			gridData.setSme(fields[6]);
			return gridData;
		}
	}
}
		
	

