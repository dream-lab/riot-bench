package in.dream_lab.bm.stream_iot.tasks.io;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

public class SQLBlobUploadTask extends AbstractTask<String,Float> {

	private static final Object SETUP_LOCK = new Object();
	private static String connStr ;
	private static String tableName ;
	private static boolean doneSetup = false;
	private static String query ;   
	private static String  username;
	private static String  password;
	private  Connection conn;
	private static String dirPath ;
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) 
			{ 
				connStr = p_.getProperty("IO.SQL.CONN_STR"); 
				username =  p_.getProperty("IO.SQL.USER_NAME");
				password = p_.getProperty("IO.SQL.PASSWORD");
				query = p_.getProperty("IO.SQL.QUERY");
				dirPath = p_.getProperty("IO.SQL_BLOB_UPLOAD.DIR_PATH");
				doneSetup = true;
			}
		}	
		try
		{
			Class.forName("com.mysql.cj.jdbc.Driver");
			//STEP 3: Open a connection
			conn = DriverManager.getConnection(connStr, username, password);

		}
		catch(Exception e)
		{
			l.warn("Exception in blob upload task "+e.getMessage());
		}		
	}
	
	@Override
	protected Float doTaskLogic(Map<String, String> map) 
	{
		try
		{
			String filePath = map.get(AbstractTask.DEFAULT_KEY);
			String temp [] = filePath.split("/");
			int len = temp.length;
			PreparedStatement preparedStatement = conn.prepareStatement(query); 
			File f1=new File(filePath);
			FileInputStream fin=new FileInputStream(f1);
			preparedStatement.setString(1, temp[len-1] );
			preparedStatement.setBinaryStream(2, fin, (int) f1.length());;
			preparedStatement.executeUpdate();
			return 0.0f;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}


	}
