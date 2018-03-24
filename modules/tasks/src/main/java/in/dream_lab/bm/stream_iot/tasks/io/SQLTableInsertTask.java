package in.dream_lab.bm.stream_iot.tasks.io;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;


import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;

public class SQLTableInsertTask extends AbstractTask<String,Float>
{	
	private static final Object SETUP_LOCK = new Object();
	private static String connStr ;
	private static String tableName ;
	private static boolean doneSetup = false;
	private static String query ;   
	private static String  username;
	private static String  password;
	private Connection conn;
	@Override
	public void setup(Logger l_, Properties p_) 
	{
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) 
		{
			if(!doneSetup)
			{ 
				connStr = p_.getProperty("IO.SQL.CONN_STR"); 
				username =  p_.getProperty("IO.SQL.USER_NAME");
				password = p_.getProperty("IO.SQL.PASSWORD");
				query = p_.getProperty("IO.SQL.QUERY");		
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
					l.warn("Exception in SQL table insert "+e.getMessage());
				}
		}
	

	@Override
	public Float doTaskLogic(Map map) 
	{
		String tuple;
		PreparedStatement preparedStatement = null;
		try 
		{
			for(int i = 0; i < map.size(); i++)
			{
				  String text = (String) map.get(String.valueOf(i));
				  String []temp = text.split(",");
				  String id = temp[0];
				  preparedStatement = conn.prepareStatement(query);
			      preparedStatement.setString(1, id );
			      preparedStatement.setString(2, text.trim());
			      preparedStatement.executeUpdate();
			}
			return null;
		}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
			return 0.1f;
	}
	
	public float tearDown() 
	{
		try 
		{
			conn.close();
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		return 0.0f;
	
	}

}
