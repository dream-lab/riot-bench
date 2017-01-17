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


public class AzureTableRangeQueryTaskFIT extends AbstractTask {

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
		Iterable<FIT_data> result = getAzTableRangeByKeyFIT(cloudTbl, partitionKey,rowKeyStart,rowKeyEnd, l);
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
	public static Iterable<FIT_data> getAzTableRangeByKeyFIT(CloudTable cloudTable, String partitionKey, String rowkeyStart, String rowkeyEnd, Logger l) {

		try {



// filters
			System.out.println("getAzTableRowByKey-"+rowkeyStart+","+rowkeyEnd);

			// Create a filter condition where the partition key is "Smith".
			String partitionFilter = TableQuery.generateFilterCondition(
					"PartitionKey",
					TableQuery.QueryComparisons.EQUAL,
					"fit_range");

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

			TableQuery<FIT_data> rangeQuery =
					TableQuery.from(FIT_data.class)
							.where(combinedFilter);

			Iterable<FIT_data> queryRes = cloudTable.execute(rangeQuery);

//			// Loop through the results, displaying information about the entity
//			for (FIT_data entity : queryRes) {
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

	public static final class FIT_data extends TableServiceEntity
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
		public static  FIT_data parseString(String s, long counter)
		{
			FIT_data obj = new FIT_data();
			String fields[] = s.split(",");
			obj.rowKey = fields[0]+"-" +fields[1];
			obj.partitionKey = "fit_range";
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
			obj.setGyro_arm_x(fields[19]);
			obj.setGyro_arm_y(fields[20]);
			obj.setGyro_arm_z(fields[21]);
			obj.setMagnetometer_arm_x(fields[22]);
			obj.setMagnetometer_arm_y(fields[23]);
			obj.setMagnetometer_arm_z(fields[24]);
			obj.setLabel(fields[25]);
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


}
